from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.shared import execution
from pipeline.shared.inmet_header import validate_headers
from pipeline.bronze.bronze_storage import prepare_inputs, write_bronze


@execution.step("bronze", "inmet", before_inputs=prepare_inputs)
def ingest_inmet(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Lê os CSVs horários do INMET e grava a Bronze em Parquet, separada por UF."""

    schema = StructType(
        [
            StructField("observation_date", StringType(), True),
            StructField("observation_time_utc", StringType(), True),
            StructField("precipitation_mm", StringType(), True),
            StructField("station_pressure_mb", StringType(), True),
            StructField("station_pressure_max_mb", StringType(), True),
            StructField("station_pressure_min_mb", StringType(), True),
            StructField("global_radiation_kj_m2", StringType(), True),
            StructField("air_temperature_c", StringType(), True),
            StructField("dew_point_temperature_c", StringType(), True),
            StructField("air_temperature_max_c", StringType(), True),
            StructField("air_temperature_min_c", StringType(), True),
            StructField("dew_point_temperature_max_c", StringType(), True),
            StructField("dew_point_temperature_min_c", StringType(), True),
            StructField("relative_humidity_max_percent", StringType(), True),
            StructField("relative_humidity_min_percent", StringType(), True),
            StructField("relative_humidity_percent", StringType(), True),
            StructField("wind_direction_degrees", StringType(), True),
            StructField("wind_gust_ms", StringType(), True),
            StructField("wind_speed_ms", StringType(), True),
            StructField("trailing_empty_field", StringType(), True),
        ]
    )

    # Lê cada arquivo inteiro para separar os metadados das medições.
    input_path = input_directory.resolve().as_posix() + "/*.CSV"

    files = spark.read.format("binaryFile").load(input_path)

    # O INMET usa Latin-1; a decodificação mantém os acentos do arquivo original.
    files = files.withColumn(
        "source_lines",
        F.split(F.decode(F.col("content"), "ISO-8859-1"), r"\r?\n", -1),
    )

    # Confere o contrato antes de atribuir nomes às medições por posição.
    headers = files.select(
        F.col("path").alias("source_file"),
        F.try_element_at(F.col("source_lines"), F.lit(9)).alias("source_header"),
    )

    validate_headers(headers)

    metadata_fields = [
        "station_region",
        "state_code",
        "station_name",
        "station_code",
        "station_latitude",
        "station_longitude",
        "station_altitude",
        "station_founded_at",
    ]

    metadata_columns = [F.col("path").alias("source_file")]

    # As oito primeiras linhas contêm os dados da estação, após o ponto e vírgula.
    for line_number, field_name in enumerate(metadata_fields, start=1):

        metadata_line = F.element_at(F.col("source_lines"), line_number)
        metadata_value = F.regexp_extract(metadata_line, r"^[^;]*;(.*)$", 1)
        metadata_columns.append(metadata_value.alias(field_name))

    # Preserva o cabeçalho original com os nomes e as unidades das medições.
    metadata_columns.append(F.element_at(F.col("source_lines"), 9).alias("source_header"))

    lines = files.select(
        *metadata_columns,
        F.posexplode(F.col("source_lines")).alias("line_index", "source_line"),
    )

    # As medições começam na décima linha; descarta apenas linhas vazias.
    lines = lines.filter((F.col("line_index") >= 9) & (F.col("source_line") != ""))
    lines = lines.withColumn("source_row_number", F.col("line_index") + 1)

    # Mantém datas, vírgulas decimais e marcadores como -9999 em formato de texto.
    observations = lines.withColumn(
        "observation",
        F.from_csv(
            F.col("source_line"),
            schema.simpleString(),
            {"sep": ";", "mode": "FAILFAST", "nullValue": "\u0000", "emptyValue": ""},
        ),
    )

    # A linha original também é mantida para conferir campos vazios e valores da fonte.
    observations = observations.select("*", "observation.*").drop("observation", "line_index")

    # Substitui a saída anterior desta fonte pelos dados da nova execução.
    write_bronze(observations, output_directory, "inmet")

    return observations
