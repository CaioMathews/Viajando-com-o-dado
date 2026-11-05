from pathlib import Path
import os
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from pipeline.shared.quality import measurement_fields, validate_quality
from pipeline.shared.storage import write_parquet
from pyspark.sql import SparkSession


class QualityRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"]=sys.executable
        builder=SparkSession.builder.master("local[2]").appName("Testes de regressao da qualidade").config("spark.sql.shuffle.partitions","2").config("spark.pyspark.python",sys.executable)

        if os.name=="nt":
            jar=ROOT/"pipeline/bronze/lib/hadoop-bare-naked-local-fs-0.1.0.jar"
            builder=builder.config("spark.driver.extraClassPath",str(jar)).config("spark.executor.extraClassPath",str(jar)).config("spark.hadoop.fs.file.impl","com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")

        cls.spark=builder.getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self,source,values,flags=None,invalid=None,missing=None):
        fields=measurement_fields(source)
        invalid=[] if invalid is None else invalid
        flags=[] if flags is None else flags
        record=[]
        schema=[]
        inferred_missing=[]

        for field in fields:
            value=values.get(field)
            record.append(value)
            schema.append(field+" double")

            if value is None and field not in invalid:
                inferred_missing.append(field)

        record.extend([inferred_missing if missing is None else missing,invalid,flags])
        schema.extend(["missing_fields array<string>","invalid_fields array<string>","quality_flags array<string>"])

        return self.spark.createDataFrame([tuple(record)],", ".join(schema))

    def test_negative_precipitation_without_alert_is_rejected(self):
        for source,field in [("inmet","precipitation_mm"),("open_meteo","precipitation_sum_mm")]:
            with self.subTest(source=source),self.assertRaises(ValueError):

                validate_quality(self.frame(source,{field:-2.0}),source)

    def test_correctly_flagged_physical_error_preserves_other_variables(self):
        for source,rain,temperature in [("inmet","precipitation_mm","air_temperature_c"),("open_meteo","precipitation_sum_mm","temperature_mean_c")]:
            frame=self.frame(source,{rain:-2.0,temperature:25.0},["negative_precipitation"],[rain])
            validate_quality(frame,source)
            self.assertEqual(frame.first()[temperature],25.0)

    def test_obsolete_alert_is_rejected(self):
        with self.assertRaises(ValueError):

            validate_quality(self.frame("open_meteo",{"precipitation_sum_mm":2.0},["negative_precipitation"],["precipitation_sum_mm"]),"open_meteo")

    def test_null_without_missing_or_invalid_marker_is_rejected(self):
        with self.assertRaises(ValueError):

            validate_quality(self.frame("open_meteo",{},missing=[]),"open_meteo")

    def test_numeric_error_converted_to_null_is_accepted(self):
        validate_quality(self.frame("open_meteo",{},["invalid_numeric:temperature_mean_c"],["temperature_mean_c"]),"open_meteo")

    def test_zero_rain_and_negative_temperature_are_valid(self):
        validate_quality(self.frame("open_meteo",{"precipitation_sum_mm":0.0,"temperature_mean_c":-2.0}),"open_meteo")

    def test_inverted_temperature_bounds_require_all_affected_markers(self):
        values={"temperature_min_c":30.0,"temperature_max_c":20.0}

        with self.assertRaises(ValueError):

            validate_quality(self.frame("open_meteo",values),"open_meteo")

        validate_quality(self.frame("open_meteo",values,["temperature_min_above_max"],["temperature_min_c","temperature_max_c"]),"open_meteo")

    def test_unknown_quality_marker_is_rejected(self):
        with self.assertRaises(ValueError):

            validate_quality(self.frame("open_meteo",{},["unknown-rule"]),"open_meteo")

    def test_real_parquet_replacement(self):
        with tempfile.TemporaryDirectory(prefix="parquet-regression-") as directory:

            output=Path(directory)/"output"
            write_parquet(self.spark.range(3),output,"test",False)
            write_parquet(self.spark.range(5),output,"test",False)
            self.assertEqual(self.spark.read.parquet(output.as_posix()).count(),5)


if __name__=="__main__":
    unittest.main()
