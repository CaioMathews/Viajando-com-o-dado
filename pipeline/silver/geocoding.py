from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet


@execution.step("silver", "geocoding")
def transform_geocoding(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Padroniza as coordenadas da Bronze e grava a Silver por UF."""

    input_directory = input_directory.resolve()
    output_directory = output_directory.resolve()

    # Impede que a gravação substitua a entrada ou uma pasta que a contenha.
    if (
        input_directory == output_directory
        or input_directory in output_directory.parents
        or output_directory in input_directory.parents
    ):
        raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    coordinates = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
    )

    # Mantém o nome com acentos e a origem do registro para rastreabilidade.
    coordinates = coordinates.select(
        F.expr("try_cast(municipality_id AS BIGINT)").alias("municipality_id"),
        F.regexp_replace(F.trim(F.col("name")), r"\s+", " ").alias("name"),
        F.upper(F.trim(F.col("state_code"))).alias("state_code"),
        F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
        F.col("source_file"),
    )

    state_codes = [
        "AC",
        "AL",
        "AP",
        "AM",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MT",
        "MS",
        "MG",
        "PA",
        "PB",
        "PR",
        "PE",
        "PI",
        "RJ",
        "RN",
        "RS",
        "RO",
        "RR",
        "SC",
        "SP",
        "SE",
        "TO",
    ]

    # Os limites verificam coordenadas globais; não comprovam a localização na UF.
    invalid_coordinates = coordinates.filter(
        F.col("municipality_id").isNull()
        | ~F.col("municipality_id").between(1000000, 9999999)
        | F.col("name").isNull()
        | (F.col("name") == "")
        | F.col("state_code").isNull()
        | ~F.col("state_code").isin(state_codes)
        | F.col("latitude").isNull()
        | F.isnan("latitude")
        | ~F.col("latitude").between(-90, 90)
        | F.col("longitude").isNull()
        | F.isnan("longitude")
        | ~F.col("longitude").between(-180, 180)
    )

    if has_rows(invalid_coordinates):
        raise ValueError(
            "Existem IDs, nomes, UFs ou coordenadas inválidos. Revise a Bronze antes de gerar a Silver."
        )

    if coordinates.limit(1).count() == 0:
        raise ValueError("A entrada não contém coordenadas para gerar a Silver.")

    # Remove somente linhas inteiramente iguais, incluindo sua origem.
    coordinates = coordinates.dropDuplicates()

    # Origens diferentes podem repetir o mesmo município sem alterar seus dados.
    municipality_values = coordinates.select(
        "municipality_id",
        "name",
        "state_code",
        "latitude",
        "longitude",
    ).distinct()

    conflicts = municipality_values.groupBy("municipality_id").count().filter(F.col("count") > 1)

    if has_rows(conflicts):
        raise ValueError(
            "Existem dados conflitantes para o mesmo município. Revise a Bronze antes de gerar a Silver."
        )

    # Agrupa as origens sem escolher arbitrariamente uma delas.
    coordinates = coordinates.groupBy(
        "municipality_id",
        "name",
        "state_code",
        "latitude",
        "longitude",
    ).agg(F.sort_array(F.collect_set("source_file")).alias("source_files"))

    # A coluna auxiliar permite manter state_code dentro de cada Parquet.
    write_parquet(coordinates, output_directory, "coordinates")

    return coordinates
