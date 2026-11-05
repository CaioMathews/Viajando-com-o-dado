from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet


@execution.step("silver", "ibge_population")
def transform_population(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Extrai os municípios das linhas da planilha na Bronze e grava a Silver por UF."""

    input_directory = input_directory.resolve()
    output_directory = output_directory.resolve()

    # Impede que a gravação substitua a entrada ou uma pasta que a contenha.
    if (
        input_directory == output_directory
        or input_directory in output_directory.parents
        or output_directory in input_directory.parents
    ):
        raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    population_rows = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
        .withColumn(
            "row_values",
            F.from_json("row_values_json", ArrayType(StringType()), {"mode": "FAILFAST"}),
        )
    )

    if has_rows(population_rows.filter(F.col("row_values").isNull() | (F.size("row_values") != 6))):
        raise ValueError(
            "Existem linhas sem as seis células esperadas. Revise o formato da planilha na Bronze."
        )

    population_rows = population_rows.select(
        "source_file",
        "source_sheet",
        "source_row_number",
        F.upper(F.trim(F.get("row_values", 0))).alias("state_code"),
        F.trim(F.get("row_values", 1)).alias("state_id_raw"),
        F.trim(F.get("row_values", 2)).alias("municipality_code_raw"),
        F.regexp_replace(F.trim(F.get("row_values", 3)), r"\s+", " ").alias("name"),
        F.trim(F.get("row_values", 4)).alias("population_raw"),
        F.trim(F.get("row_values", 5)).alias("population_note"),
    )

    # O ano da publicação ou do nome do arquivo não substitui a data de referência.
    reference_dates = (
        population_rows.filter(F.col("state_code").startswith("ESTIMATIVAS DA POPULA"))
        .withColumn(
            "reference_year",
            F.regexp_extract(
                "state_code", r"DATA DE REFERÊNCIA EM 1[º°]? DE JULHO DE ([0-9]{4})$", 1
            ),
        )
        .withColumn("reference_date", F.expr("try_cast(concat(reference_year, '-07-01') AS DATE)"))
        .select("source_file", "source_sheet", "reference_date")
        .distinct()
    )

    if has_rows(reference_dates.filter(F.col("reference_date").isNull())):
        raise ValueError("Não foi possível interpretar a data de referência no título da planilha.")

    if has_rows(
        reference_dates.groupBy("source_file", "source_sheet").count().filter(F.col("count") > 1)
    ):
        raise ValueError("Existem datas de referência conflitantes na mesma planilha.")

    integer_pattern = r"^[+]?[0-9]+(?:\.0+)?$"
    is_header = (F.col("state_code") == "UF") & (F.col("state_id_raw") == "COD. UF")
    has_numeric_keys = F.col("state_id_raw").rlike(integer_pattern) & F.col(
        "municipality_code_raw"
    ).rlike(integer_pattern)
    has_population = (F.length(F.coalesce(F.col("name"), F.lit(""))) > 0) & (
        F.length(F.coalesce(F.col("population_raw"), F.lit(""))) > 0
    )

    # Mantém candidatos com campos defeituosos para validá-los, sem descartá-los como notas.
    population = population_rows.filter(
        ~is_header & (F.col("state_code").rlike("^[A-Z]{2}$") | has_numeric_keys | has_population)
    ).join(reference_dates, ["source_file", "source_sheet"], "left")

    population = population.withColumn(
        "state_id",
        F.when(
            F.col("state_id_raw").rlike(integer_pattern),
            F.expr("try_cast(try_cast(state_id_raw AS DECIMAL(20, 0)) AS BIGINT)"),
        ),
    )
    population = population.withColumn(
        "municipality_code_number",
        F.when(
            F.col("municipality_code_raw").rlike(integer_pattern),
            F.expr("try_cast(try_cast(municipality_code_raw AS DECIMAL(20, 0)) AS BIGINT)"),
        ),
    )
    population = population.withColumn(
        "municipality_code",
        F.when(
            F.col("municipality_code_number").between(0, 99999),
            F.lpad(F.col("municipality_code_number").cast("string"), 5, "0"),
        ),
    )
    population = population.withColumn(
        "municipality_id",
        F.expr("try_cast(concat(cast(state_id AS STRING), municipality_code) AS BIGINT)"),
    )

    # Aceita a notação científica produzida na leitura JSON, mas não arredonda habitantes.
    population = population.withColumn(
        "population_number", F.expr("try_cast(population_raw AS DECIMAL(38, 18))")
    )
    population = population.withColumn(
        "population_estimate",
        F.when(
            F.col("population_number") == F.floor("population_number"),
            F.expr("try_cast(population_number AS BIGINT)"),
        ),
    )

    state_ids = {
        "RO": 11,
        "AC": 12,
        "AM": 13,
        "RR": 14,
        "PA": 15,
        "AP": 16,
        "TO": 17,
        "MA": 21,
        "PI": 22,
        "CE": 23,
        "RN": 24,
        "PB": 25,
        "PE": 26,
        "AL": 27,
        "SE": 28,
        "BA": 29,
        "MG": 31,
        "ES": 32,
        "RJ": 33,
        "SP": 35,
        "PR": 41,
        "SC": 42,
        "RS": 43,
        "MS": 50,
        "MT": 51,
        "GO": 52,
        "DF": 53,
    }
    state_mapping = []

    for state_code, state_id in state_ids.items():

        state_mapping.extend([F.lit(state_code), F.lit(state_id)])

    expected_state_id = F.create_map(*state_mapping)[F.col("state_code")]
    invalid_population = population.filter(
        expected_state_id.isNull()
        | ~F.col("state_id").eqNullSafe(expected_state_id)
        | F.col("municipality_id").isNull()
        | ~F.col("municipality_id").between(1000000, 9999999)
        | F.col("name").isNull()
        | (F.col("name") == "")
        | F.col("population_estimate").isNull()
        | (F.col("population_estimate") <= 0)
        | F.col("reference_date").isNull()
        | F.col("source_file").isNull()
        | F.col("source_sheet").isNull()
        | F.col("source_row_number").isNull()
        | (F.col("source_row_number") <= 0)
    )

    if has_rows(invalid_population):
        raise ValueError(
            "Existem códigos, nomes, populações, datas ou campos de origem inválidos. Revise a Bronze antes de gerar a Silver."
        )

    if population.limit(1).count() == 0:
        raise ValueError("A entrada não contém municípios para gerar a Silver.")

    # Preserva as chamadas de nota da sexta coluna, como (1), sem tratá-las como erro.
    value_columns = [
        "municipality_id",
        "name",
        "state_code",
        "state_id",
        "population_estimate",
        "reference_date",
        "population_note",
    ]
    conflicts = (
        population.select(*value_columns)
        .distinct()
        .groupBy("municipality_id", "reference_date")
        .count()
        .filter(F.col("count") > 1)
    )

    if has_rows(conflicts):
        raise ValueError(
            "Existem dados conflitantes para o mesmo município e data de referência. Revise a Bronze antes de gerar a Silver."
        )

    # Reúne as origens das duplicatas equivalentes, incluindo aba e número da linha.
    population = population.groupBy(*value_columns).agg(
        F.sort_array(
            F.collect_set(F.struct("source_file", "source_sheet", "source_row_number"))
        ).alias("sources"),
    )

    # A coluna auxiliar permite manter state_code dentro de cada Parquet.
    write_parquet(population, output_directory, "population")

    return population
