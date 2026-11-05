from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from pipeline.shared import execution
from pipeline.bronze.bronze_storage import prepare_inputs, write_bronze


@execution.step("bronze", "open_meteo", before_inputs=prepare_inputs)
def ingest_open_meteo(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Lê os JSONs de clima e grava a Bronze em Parquet, separada por UF."""

    daily_schema = StructType(
        [
            StructField("time", ArrayType(StringType(), True), True),
            StructField("temperature_2m_mean", ArrayType(DoubleType(), True), True),
            StructField("temperature_2m_min", ArrayType(DoubleType(), True), True),
            StructField("temperature_2m_max", ArrayType(DoubleType(), True), True),
            StructField("precipitation_sum", ArrayType(DoubleType(), True), True),
            StructField("wind_speed_10m_max", ArrayType(DoubleType(), True), True),
        ]
    )

    # Mantém os metadados, as unidades e as listas diárias da resposta original.
    weather_schema = StructType(
        [
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("generationtime_ms", DoubleType(), True),
            StructField("utc_offset_seconds", LongType(), True),
            StructField("timezone", StringType(), True),
            StructField("timezone_abbreviation", StringType(), True),
            StructField("elevation", DoubleType(), True),
            StructField("daily_units", MapType(StringType(), StringType(), True), True),
            StructField("daily", daily_schema, True),
        ]
    )

    schema = StructType(
        [
            StructField("municipality_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("weather", weather_schema, True),
        ]
    )

    # Os JSONs contêm listas de registros distribuídas em várias linhas.
    input_path = input_directory.resolve().as_posix() + "/*.json"

    weather = (
        spark.read.schema(schema)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(input_path)
    )

    # Preserva a origem e identifica a UF pelo nome do arquivo, como SP.json.
    weather = weather.withColumn("source_file", F.input_file_name())
    weather = weather.withColumn(
        "state_code",
        F.regexp_extract(F.col("source_file"), r"([A-Z]{2})\.json$", 1),
    )

    # Substitui a saída anterior desta fonte pelos dados da nova execução.
    write_bronze(weather, output_directory, "weather")

    return weather
