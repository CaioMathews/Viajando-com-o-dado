from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from pipeline.shared import execution
from pipeline.bronze.bronze_storage import prepare_inputs, write_bronze


@execution.step("bronze", "geocoding", before_inputs=prepare_inputs)
def ingest_geocoding(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Lê os JSONs de coordenadas e grava a Bronze em Parquet, separada por UF."""

    schema = StructType(
        [
            StructField("municipality_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )

    # Os JSONs contêm listas de registros distribuídas em várias linhas.
    input_path = input_directory.resolve().as_posix() + "/*.json"

    coordinates = (
        spark.read.schema(schema)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(input_path)
    )

    # Preserva a origem e identifica a UF pelo nome do arquivo, como SP.json.
    coordinates = coordinates.withColumn("source_file", F.input_file_name())
    coordinates = coordinates.withColumn(
        "state_code",
        F.regexp_extract(F.col("source_file"), r"([A-Z]{2})\.json$", 1),
    )

    # Substitui a saída anterior desta fonte pelos dados da nova execução.
    write_bronze(coordinates, output_directory, "coordinates")

    return coordinates
