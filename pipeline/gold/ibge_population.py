from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet


@execution.step("gold", "ibge_population")
def build_population(
    spark: SparkSession,
    input_directory: Path,
    municipalities_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Integra a população das capitais ao IBGE, mantendo uma linha por capital e data de referência."""

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

    population = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
    )

    municipalities = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(municipalities_directory.as_posix())
    )

    # A data de referência vem da estimativa, sem inferência pelo nome do arquivo.
    if population.limit(1).count() == 0 or has_rows(
        population.filter(F.col("reference_date").isNull())
    ):
        raise ValueError(
            "A entrada de população está vazia ou contém datas de referência ausentes."
        )

    # Seleciona capitais pelos IDs oficiais, sem restringir previamente a Silver.
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

    selected_population = population.join(
        capital_reference.select("municipality_id"), "municipality_id", "inner"
    )
    selected_municipalities = municipalities.join(
        capital_reference.select("municipality_id"), "municipality_id", "inner"
    )

    repeated_population = (
        selected_population.groupBy("municipality_id", "reference_date")
        .count()
        .filter(F.col("count") > 1)
    )
    repeated_municipalities = (
        selected_municipalities.groupBy("municipality_id").count().filter(F.col("count") > 1)
    )

    if has_rows(repeated_population) or has_rows(repeated_municipalities):
        raise ValueError(
            "Existem capitais repetidas no cadastro ou na população da mesma data. Revise a Silver antes de gerar a Gold."
        )

    selected_population = selected_population.select(
        "municipality_id",
        "reference_date",
        "population_estimate",
        "population_note",
        F.col("name").alias("population_name"),
        F.col("state_code").alias("population_state_code"),
        F.col("state_id").alias("population_state_id"),
        F.col("sources").alias("population_sources"),
    )
    selected_municipalities = selected_municipalities.select(
        "municipality_id",
        "name",
        "state_code",
        "state_id",
        "state_name",
        "region_code",
        "region_name",
        F.col("source_files").alias("municipality_source_files"),
    )

    # Exige todas as capitais em cada data presente na entrada, inclusive nas cargas de vários anos.
    reference_dates = population.select("reference_date").distinct()
    expected_capitals = capital_reference.crossJoin(reference_dates)
    capital_population = expected_capitals.join(
        selected_municipalities, "municipality_id", "left"
    ).join(selected_population, ["municipality_id", "reference_date"], "left")

    invalid = (
        ~F.col("name").eqNullSafe(F.col("expected_name"))
        | ~F.col("population_name").eqNullSafe(F.col("name"))
        | ~F.col("state_code").eqNullSafe(F.col("expected_state_code"))
        | ~F.col("population_state_code").eqNullSafe(F.col("state_code"))
        | ~F.col("population_state_id").eqNullSafe(F.col("state_id"))
        | ~F.substring(F.col("municipality_id").cast("string"), 1, 2).eqNullSafe(
            F.col("state_id").cast("string")
        )
        | F.col("population_estimate").isNull()
        | (F.col("population_estimate") <= 0)
    )

    for column_name in ["state_name", "region_code", "region_name"]:

        invalid = invalid | F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")

    for column_name in ["population_sources", "municipality_source_files"]:

        invalid = invalid | F.col(column_name).isNull() | (F.size(column_name) == 0)

    if has_rows(capital_population.filter(invalid)):
        raise ValueError(
            "Existem capitais sem estimativa válida ou divergências com o cadastro do IBGE. Revise a Silver antes de gerar a Gold."
        )

    # Preserva valores, chamadas de nota e datas; não soma estimativas de anos diferentes.
    capital_population = capital_population.select(
        "municipality_id",
        "name",
        "state_code",
        "state_id",
        "state_name",
        "region_code",
        "region_name",
        "reference_date",
        "population_estimate",
        "population_note",
        "population_sources",
        "municipality_source_files",
    ).withColumn("is_capital", F.lit(True))

    write_parquet(capital_population, output_directory, "population")

    return capital_population
