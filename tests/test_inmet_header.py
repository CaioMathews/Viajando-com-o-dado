"""Impede que mudanças no cabeçalho do INMET sejam interpretadas como outras medições."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.bronze import inmet as bronze
from pipeline.silver import inmet as silver
from pipeline.shared import execution
from pipeline.shared.inmet_header import validate_headers

HEADER = (ROOT / "tests/fixtures/inmet_header.txt").read_text(encoding="utf-8").strip()
METADATA = [
    "REGIAO:;SE",
    "UF:;SP",
    "ESTACAO:;SAO PAULO",
    "CODIGO:;A999",
    "LATITUDE:;-23,55",
    "LONGITUDE:;-46,63",
    "ALTITUDE:;700",
    "FUNDACAO:;2000-01-01",
]
VALUES = [
    "2025/01/01",
    "1200 UTC",
    "0,0",
    "900",
    "901",
    "899",
    "0",
    "20",
    "15",
    "21",
    "19",
    "16",
    "14",
    "90",
    "70",
    "80",
    "180",
    "3",
    "2",
    "",
]


class InmetHeaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        jar = ROOT / "pipeline/bronze/lib/hadoop-bare-naked-local-fs-0.1.0.jar"
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("Contrato INMET")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
        )

        if os.name == "nt":
            builder = (
                builder.config("spark.driver.extraClassPath", str(jar))
                .config("spark.executor.extraClassPath", str(jar))
                .config(
                    "spark.hadoop.fs.file.impl",
                    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
                )
            )

        cls.spark = builder.getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="inmet-header-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        environment = patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "1"})
        environment.start()
        self.addCleanup(environment.stop)
        status = patch.object(execution, "STATUS_PATH", self.directory / "status.json")
        status.start()
        self.addCleanup(status.stop)

    def write_csv(self, directory, header, values):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "station.CSV").write_bytes(
            ("\r\n".join(METADATA + [header, ";".join(values)]) + "\r\n").encode("latin-1")
        )

    def test_real_headers_in_both_periods_match_contract(self):
        records = []

        for period in ["2025", "boundary_2025"]:

            for path in sorted((ROOT / "data/inmet" / period).glob("*.CSV")):

                with path.open(encoding="latin-1") as handle:

                    for number, line in enumerate(handle, start=1):

                        if number == 9:
                            records.append((str(path), line.rstrip("\r\n")))
                            break

        if not records:
            self.skipTest("CSVs locais do INMET não disponíveis.")

        validate_headers(
            self.spark.createDataFrame(records, "source_file string, source_header string")
        )

    def test_cosmetic_header_variations_preserve_values(self):
        raw = self.directory / "raw"
        self.write_csv(raw, "  " + HEADER.lower().replace(";", " ;  ") + "  ", VALUES)
        bronze.ingest_inmet(self.spark, raw, self.directory / "bronze")
        result = silver.transform_inmet(
            self.spark, self.directory / "bronze", self.directory / "silver"
        )
        row = result.select("precipitation_mm", "wind_speed_ms", "quality_flags").first()
        self.assertEqual(row["precipitation_mm"], 0.0)
        self.assertEqual(row["wind_speed_ms"], 2.0)
        self.assertEqual(row["quality_flags"], [])

    def test_wrong_headers_fail_and_preserve_existing_bronze(self):
        columns = HEADER.split(";")
        columns[2], columns[18] = columns[18], columns[2]
        swapped_values = list(VALUES)
        swapped_values[2], swapped_values[18] = swapped_values[18], swapped_values[2]
        variants = [
            ("swapped", ";".join(columns), swapped_values),
            (
                "wind_unit",
                HEADER.replace("VELOCIDADE HORARIA (m/s)", "VELOCIDADE HORARIA (km/h)"),
                VALUES,
            ),
            ("temperature_unit", HEADER.replace("(°C)", "(°F)"), VALUES),
            ("missing_column", ";".join(HEADER.split(";")[1:]), VALUES),
            ("empty", "", VALUES),
        ]
        output = self.directory / "bronze"
        self.spark.createDataFrame([("previous",)], "version string").write.parquet(
            output.as_posix()
        )

        for name, header, values in variants:

            with self.subTest(name=name):

                raw = self.directory / name
                self.write_csv(raw, header, values)

                with self.assertRaisesRegex(ValueError, "Cabeçalho INMET incompatível"):

                    with execution.run_context() as run:

                        bronze.ingest_inmet(self.spark, raw, output)

                self.assertEqual(
                    self.spark.read.parquet(output.as_posix()).first()["version"], "previous"
                )
                self.assertEqual(run["status"], "failed")
                self.assertEqual(run["nodes"]["bronze:inmet_daily"]["status"], "failed")

    def test_silver_blocks_incompatible_existing_bronze(self):
        source = self.directory / "bronze"
        self.spark.createDataFrame(
            [("old.CSV", HEADER.replace("(m/s)", "(km/h)"))],
            "source_file string, source_header string",
        ).write.parquet(source.as_posix())

        with self.assertRaisesRegex(ValueError, "Cabeçalho INMET incompatível"):

            silver.transform_inmet(self.spark, source, self.directory / "silver")

        self.assertFalse((self.directory / "silver").exists())

    def test_silver_requires_original_header(self):
        source = self.directory / "bronze"
        self.spark.createDataFrame([("old.CSV",)], "source_file string").write.parquet(
            source.as_posix()
        )

        with self.assertRaisesRegex(ValueError, "Cabeçalho INMET ausente"):

            silver.transform_inmet(self.spark, source, self.directory / "silver")

        self.assertFalse((self.directory / "silver").exists())


if __name__ == "__main__":
    unittest.main()
