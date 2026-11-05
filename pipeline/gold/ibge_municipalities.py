from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet


@execution.step("gold", "ibge_municipalities")
def build_municipalities(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Prepara o cadastro das 27 capitais para integrar as tabelas analíticas da Gold."""

    input_directory = input_directory.resolve()
    output_directory = output_directory.resolve()

    if (
        input_directory == output_directory
        or input_directory in output_directory.parents
        or output_directory in input_directory.parents
    ):
        raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    municipalities = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
    )

    # Identifica as capitais pelo ID oficial, sem considerar qualquer município da UF como capital.
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

    selected = municipalities.join(
        capital_reference.select("municipality_id"), "municipality_id", "inner"
    )
    repeated_ids = selected.groupBy("municipality_id").count().filter(F.col("count") > 1)

    if has_rows(repeated_ids):
        raise ValueError(
            "Existem capitais repetidas na Silver. Revise a origem antes de gerar a Gold."
        )

    # A referência à esquerda permite identificar inclusive capitais completamente ausentes.
    capitals = capital_reference.join(selected, "municipality_id", "left")

    invalid = (
        ~F.col("state_code").eqNullSafe(F.col("expected_state_code"))
        | ~F.col("name").eqNullSafe(F.col("expected_name"))
        | ~F.substring(F.col("municipality_id").cast("string"), 1, 2).eqNullSafe(
            F.col("state_id").cast("string")
        )
        | F.col("region_id").isNull()
        | ~F.col("region_id").eqNullSafe(F.floor(F.col("state_id") / 10))
    )

    for column_name in [
        "state_name",
        "region_code",
        "region_name",
        "immediate_region_name",
        "intermediate_region_name",
    ]:

        invalid = invalid | F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")

    for column_name in ["immediate_region_id", "intermediate_region_id"]:

        invalid = invalid | F.col(column_name).isNull() | (F.col(column_name) <= 0)

    invalid = invalid | F.col("source_files").isNull() | (F.size("source_files") == 0)

    if has_rows(capitals.filter(invalid)):
        raise ValueError(
            "Existem capitais ausentes ou campos divergentes no cadastro. Revise a Silver antes de gerar a Gold."
        )

    # Mantém as duas divisões regionais e as origens, com uma única linha por capital.
    capitals = capitals.select(
        "municipality_id",
        "name",
        "state_code",
        "state_id",
        "state_name",
        "region_id",
        "region_code",
        "region_name",
        "immediate_region_id",
        "immediate_region_name",
        "intermediate_region_id",
        "intermediate_region_name",
        "microregion_id",
        "microregion_name",
        "mesoregion_id",
        "mesoregion_name",
        "source_files",
    ).withColumn("is_capital", F.lit(True))

    # Preserva a UF como coluna e usa uma coluna auxiliar para criar as pastas.
    write_parquet(capitals, output_directory, "municipalities")

    return capitals
