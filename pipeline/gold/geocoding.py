from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet


@execution.step("gold", "geocoding")
def build_geocoding(
    spark: SparkSession,
    input_directory: Path,
    municipalities_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Integra as coordenadas e o cadastro do IBGE em uma tabela das 27 capitais."""

    input_directory = input_directory.resolve()
    municipalities_directory = municipalities_directory.resolve()
    output_directory = output_directory.resolve()

    for source_directory in [input_directory, municipalities_directory]:

        if (
            source_directory == output_directory
            or source_directory in output_directory.parents
            or output_directory in source_directory.parents
        ):
            raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    coordinates = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
    )
    municipalities = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(municipalities_directory.as_posix())
    )

    # A referência explícita evita considerar qualquer município como capital apenas pela UF.
    capital_records = [
        ("AC", 1200401, "Rio Branco"),
        ("AL", 2704302, "Maceió"),
        ("AM", 1302603, "Manaus"),
        ("AP", 1600303, "Macapá"),
        ("BA", 2927408, "Salvador"),
        ("CE", 2304400, "Fortaleza"),
        ("DF", 5300108, "Brasília"),
        ("ES", 3205309, "Vitória"),
        ("GO", 5208707, "Goiânia"),
        ("MA", 2111300, "São Luís"),
        ("MG", 3106200, "Belo Horizonte"),
        ("MS", 5002704, "Campo Grande"),
        ("MT", 5103403, "Cuiabá"),
        ("PA", 1501402, "Belém"),
        ("PB", 2507507, "João Pessoa"),
        ("PE", 2611606, "Recife"),
        ("PI", 2211001, "Teresina"),
        ("PR", 4106902, "Curitiba"),
        ("RJ", 3304557, "Rio de Janeiro"),
        ("RN", 2408102, "Natal"),
        ("RO", 1100205, "Porto Velho"),
        ("RR", 1400100, "Boa Vista"),
        ("RS", 4314902, "Porto Alegre"),
        ("SC", 4205407, "Florianópolis"),
        ("SE", 2800308, "Aracaju"),
        ("SP", 3550308, "São Paulo"),
        ("TO", 1721000, "Palmas"),
    ]

    capital_reference = spark.createDataFrame(
        capital_records,
        "expected_state_code string, municipality_id long, expected_name string",
    )

    # Seleciona as capitais por ID e exige uma única linha por capital em cada fonte.
    capital_coordinates = coordinates.join(
        capital_reference.select("municipality_id"), "municipality_id", "inner"
    )
    capital_municipalities = municipalities.join(
        capital_reference.select("municipality_id"), "municipality_id", "inner"
    )

    for source_data in [capital_coordinates, capital_municipalities]:

        repeated_ids = source_data.groupBy("municipality_id").count().filter(F.col("count") > 1)

        if has_rows(repeated_ids):
            raise ValueError(
                "Existem capitais repetidas na Silver. Revise a origem antes de gerar a Gold."
            )

    capital_coordinates = capital_coordinates.select(
        "municipality_id",
        F.col("name").alias("coordinate_name"),
        F.col("state_code").alias("coordinate_state_code"),
        "latitude",
        "longitude",
        F.col("source_files").alias("coordinate_source_files"),
    )

    capital_municipalities = capital_municipalities.select(
        "municipality_id",
        "name",
        "state_code",
        "state_name",
        "region_code",
        "region_name",
        F.col("source_files").alias("municipality_source_files"),
    )

    capitals = capital_reference.join(capital_municipalities, "municipality_id", "left").join(
        capital_coordinates, "municipality_id", "left"
    )

    invalid = (
        ~F.col("state_code").eqNullSafe(F.col("expected_state_code"))
        | ~F.col("coordinate_state_code").eqNullSafe(F.col("expected_state_code"))
        | ~F.col("name").eqNullSafe(F.col("expected_name"))
        | ~F.col("coordinate_name").eqNullSafe(F.col("name"))
        | F.col("latitude").isNull()
        | F.isnan("latitude")
        | ~F.col("latitude").between(-90, 90)
        | F.col("longitude").isNull()
        | F.isnan("longitude")
        | ~F.col("longitude").between(-180, 180)
    )

    for column_name in ["state_name", "region_code", "region_name"]:

        invalid = invalid | F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")

    for column_name in ["coordinate_source_files", "municipality_source_files"]:

        invalid = invalid | F.col(column_name).isNull() | (F.size(column_name) == 0)

    if has_rows(capitals.filter(invalid)):
        raise ValueError(
            "Existem capitais ausentes, coordenadas inválidas ou divergências com o IBGE. Revise a Silver antes de gerar a Gold."
        )

    # Usa os nomes oficiais do IBGE e preserva a origem de ambas as fontes.
    capitals = capitals.select(
        "municipality_id",
        "name",
        "state_code",
        "state_name",
        "region_code",
        "region_name",
        "latitude",
        "longitude",
        "coordinate_source_files",
        "municipality_source_files",
    )

    # A coluna auxiliar permite manter state_code dentro de cada Parquet.
    write_parquet(capitals, output_directory, "coordinates")

    return capitals
