from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pipeline.shared import execution
from pipeline.bronze.bronze_storage import prepare_inputs, write_bronze


@execution.step("bronze", "ibge_municipalities", before_inputs=prepare_inputs)
def ingest_municipalities(spark: SparkSession, input_directory: Path, output_directory: Path,) -> DataFrame:
    """Lê os JSONs de municípios do IBGE e grava a Bronze em Parquet, separada por UF."""

    region_schema = StructType([
        StructField("id", LongType(), True),
        StructField("sigla", StringType(), True),
        StructField("nome", StringType(), True),
    ])

    state_schema = StructType([
        StructField("id", LongType(), True),
        StructField("sigla", StringType(), True),
        StructField("nome", StringType(), True),
        StructField("regiao", region_schema, True),
    ])

    # Mesorregiões e regiões intermediárias têm a mesma estrutura de campos.
    regional_area_schema = StructType([
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("UF", state_schema, True),
    ])

    microregion_schema = StructType([
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("mesorregiao", regional_area_schema, True),
    ])

    immediate_region_schema = StructType([
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("regiao-intermediaria", regional_area_schema, True),
    ])

    # Mantém os nomes e as hierarquias originais da resposta do IBGE.
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("microrregiao", microregion_schema, True),
        StructField("regiao-imediata", immediate_region_schema, True),
    ])

    # Os JSONs contêm listas de registros distribuídas em várias linhas.
    input_path = input_directory.resolve().as_posix()

    municipalities = (
        spark.read
        .schema(schema)
        .option("pathGlobFilter", "*.json")
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(input_path)
    )

    # Preserva a origem e identifica a UF pelo nome do arquivo, como SP.json.
    municipalities = municipalities.withColumn("source_file", F.input_file_name())
    municipalities = municipalities.withColumn(
        "state_code",
        F.regexp_extract(F.col("source_file"), r"([A-Z]{2})\.json$", 1),
    )

    # Substitui a saída anterior desta fonte pelos dados da nova execução.
    write_bronze(municipalities, output_directory, "municipalities")

    return municipalities
