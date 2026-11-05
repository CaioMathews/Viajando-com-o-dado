"""Protege as fontes brutas contra destinos sobrepostos nos cinco ingestores."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.bronze import (
    bronze_storage,
    geocoding,
    ibge_municipalities,
    ibge_population,
    inmet,
    open_meteo,
)
from pipeline.shared import execution


class BronzePathTests(unittest.TestCase):
    def file_contents(self, directory):
        records = []

        for path in sorted(directory.rglob("*")):

            if path.is_file():
                records.append((path.relative_to(directory).as_posix(), path.read_bytes()))

        return records

    def test_every_ingestor_rejects_overlap_before_reading_or_recovering(self):
        functions = [
            geocoding.ingest_geocoding,
            ibge_municipalities.ingest_municipalities,
            ibge_population.ingest_population,
            inmet.ingest_inmet,
            open_meteo.ingest_open_meteo,
        ]

        with tempfile.TemporaryDirectory(prefix="bronze-path-tests-") as temporary:

            directory = Path(temporary)
            project = directory / "project"
            source = project / "raw"
            source.mkdir(parents=True)
            (source / "SP.json").write_bytes(b"raw JSON must remain untouched")
            (source / "station.CSV").write_bytes(b"raw CSV must remain untouched")
            (source / "population.xls").write_bytes(b"raw XLS must remain untouched")
            (project / "notes.txt").write_bytes(b"unrelated file must remain untouched")
            before = self.file_contents(project)
            destinations = [source, source / "bronze", project, source / ".." / "raw"]

            for enabled in ["0", "1"]:

                with (
                    patch.dict(os.environ, {"VCD_TRACKING_ENABLED": enabled}),
                    patch.object(execution, "STATUS_PATH", directory / "status.json"),
                ):

                    for function in functions:

                        for destination in destinations:

                            with self.subTest(
                                audit=enabled,
                                source=function.__name__,
                                destination=str(destination),
                            ):

                                with patch.object(bronze_storage, "recover_inputs") as recovery:

                                    with self.assertRaisesRegex(
                                        ValueError, "separadas, sem sobreposição"
                                    ):

                                        with execution.run_context() as run:

                                            # Um acesso ao Spark falharia: a validação deve ocorrer antes dele.
                                            function(object(), source, destination)

                                recovery.assert_not_called()
                                self.assertEqual(self.file_contents(project), before)
                                self.assertFalse((source / "bronze").exists())

                                if enabled == "1":
                                    self.assertEqual(run["status"], "failed")

    def test_distinct_sibling_paths_are_accepted_and_recovery_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:

            directory = Path(temporary)
            source = directory / "raw"
            source.mkdir()
            output = directory / "raw_bronze"
            parameters = {"input_directory": source, "output_directory": output}

            with patch.object(bronze_storage, "recover_inputs") as recovery:

                bronze_storage.prepare_inputs(parameters)

            recovery.assert_called_once_with(parameters)
            self.assertFalse(output.exists())

    def test_invalid_paths_block_json_recovery_before_any_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:

            data = Path(temporary) / "data"
            source = data / "open_meteo/geocoding"
            source.mkdir(parents=True)

            with patch.object(bronze_storage, "recover_inputs") as recovery:

                with self.assertRaisesRegex(ValueError, "separadas, sem sobreposição"):

                    bronze_storage.prepare_inputs(
                        {"input_directory": source, "output_directory": data}
                    )

            recovery.assert_not_called()


if __name__ == "__main__":
    unittest.main()
