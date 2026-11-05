from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet


@execution.step("silver", "ibge_municipalities")
def transform_municipalities(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Organiza as hierarquias dos municípios da Bronze e grava a Silver por UF."""

    input_directory = input_directory.resolve()
    output_directory = output_directory.resolve()

    # Impede que a gravação substitua a entrada ou uma pasta que a contenha.
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

    # Abre os objetos em colunas; as duas divisões regionais continuam distintas.
    municipalities = municipalities.select(
        F.col("id").alias("municipality_id"),
        F.col("nome").alias("name"),
        F.col("state_code"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.UF.id").alias("state_id"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.UF.sigla").alias("hierarchy_state_code"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.UF.nome").alias("state_name"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.UF.regiao.id").alias("region_id"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.UF.regiao.sigla").alias("region_code"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.UF.regiao.nome").alias("region_name"),
        F.col("`regiao-imediata`.id").alias("immediate_region_id"),
        F.col("`regiao-imediata`.nome").alias("immediate_region_name"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.id").alias("intermediate_region_id"),
        F.col("`regiao-imediata`.`regiao-intermediaria`.nome").alias("intermediate_region_name"),
        F.col("microrregiao.id").alias("microregion_id"),
        F.col("microrregiao.nome").alias("microregion_name"),
        F.col("microrregiao.mesorregiao.id").alias("mesoregion_id"),
        F.col("microrregiao.mesorregiao.nome").alias("mesoregion_name"),
        F.col("microrregiao.mesorregiao.UF.id").alias("legacy_state_id"),
        F.col("microrregiao.mesorregiao.UF.sigla").alias("legacy_state_code"),
        F.col("microrregiao.mesorregiao.UF.nome").alias("legacy_state_name"),
        F.col("microrregiao.mesorregiao.UF.regiao.id").alias("legacy_region_id"),
        F.col("microrregiao.mesorregiao.UF.regiao.sigla").alias("legacy_region_code"),
        F.col("microrregiao.mesorregiao.UF.regiao.nome").alias("legacy_region_name"),
        F.col("microrregiao").isNotNull().alias("has_legacy_hierarchy"),
        F.col("source_file"),
    )

    # Padroniza os textos sem remover acentos ou mudar os nomes próprios.
    for field in municipalities.schema.fields:

        if field.dataType.simpleString() == "string" and field.name != "source_file":
            text = F.regexp_replace(F.trim(F.col(field.name)), r"\s+", " ")

            if field.name.endswith("_code"):
                text = F.upper(text)

            municipalities = municipalities.withColumn(field.name, text)

    state_codes = {
        11: "RO",
        12: "AC",
        13: "AM",
        14: "RR",
        15: "PA",
        16: "AP",
        17: "TO",
        21: "MA",
        22: "PI",
        23: "CE",
        24: "RN",
        25: "PB",
        26: "PE",
        27: "AL",
        28: "SE",
        29: "BA",
        31: "MG",
        32: "ES",
        33: "RJ",
        35: "SP",
        41: "PR",
        42: "SC",
        43: "RS",
        50: "MS",
        51: "MT",
        52: "GO",
        53: "DF",
    }

    region_codes = {1: "N", 2: "NE", 3: "SE", 4: "S", 5: "CO"}
    state_mapping = []
    region_mapping = []

    for state_id, state_code in state_codes.items():

        state_mapping.extend([F.lit(state_id), F.lit(state_code)])

    for region_id, region_code in region_codes.items():

        region_mapping.extend([F.lit(region_id), F.lit(region_code)])

    expected_state = F.create_map(*state_mapping)[F.col("state_id")]
    expected_region = F.create_map(*region_mapping)[F.col("region_id")]

    invalid_condition = (
        F.col("municipality_id").isNull()
        | ~F.col("municipality_id").between(1000000, 9999999)
        | expected_state.isNull()
        | ~F.col("state_code").eqNullSafe(expected_state)
        | ~F.col("hierarchy_state_code").eqNullSafe(expected_state)
        | ~F.substring(F.col("municipality_id").cast("string"), 1, 2).eqNullSafe(
            F.col("state_id").cast("string")
        )
        | expected_region.isNull()
        | ~F.col("region_code").eqNullSafe(expected_region)
        | ~F.col("region_id").eqNullSafe(F.floor(F.col("state_id") / 10))
    )

    for column_name in [
        "name",
        "state_name",
        "region_name",
        "immediate_region_name",
        "intermediate_region_name",
    ]:

        invalid_condition = (
            invalid_condition | F.col(column_name).isNull() | (F.col(column_name) == "")
        )

    for column_name in ["immediate_region_id", "intermediate_region_id"]:

        invalid_condition = (
            invalid_condition | F.col(column_name).isNull() | (F.col(column_name) <= 0)
        )

    # A hierarquia antiga pode estar ausente; quando presente, precisa ser coerente.
    legacy_matches = F.lit(True)

    for legacy_column, current_column in [
        ("legacy_state_id", "state_id"),
        ("legacy_state_code", "hierarchy_state_code"),
        ("legacy_state_name", "state_name"),
        ("legacy_region_id", "region_id"),
        ("legacy_region_code", "region_code"),
        ("legacy_region_name", "region_name"),
    ]:

        legacy_matches = legacy_matches & F.col(legacy_column).eqNullSafe(F.col(current_column))

    for column_name in ["microregion_name", "mesoregion_name"]:

        legacy_matches = (
            legacy_matches & F.col(column_name).isNotNull() & (F.col(column_name) != "")
        )

    for column_name in ["microregion_id", "mesoregion_id"]:

        legacy_matches = legacy_matches & F.col(column_name).isNotNull() & (F.col(column_name) > 0)

    invalid_condition = invalid_condition | (F.col("has_legacy_hierarchy") & ~legacy_matches)

    if has_rows(municipalities.filter(invalid_condition)):
        raise ValueError(
            "Existem campos obrigatórios inválidos ou divergências entre ID, UF e hierarquias. Revise a Bronze antes de gerar a Silver."
        )

    if municipalities.limit(1).count() == 0:
        raise ValueError("A entrada não contém municípios para gerar a Silver.")

    # Após a conferência, guarda apenas uma cópia dos campos comuns de UF e região.
    municipalities = municipalities.drop(
        "hierarchy_state_code",
        "legacy_state_id",
        "legacy_state_code",
        "legacy_state_name",
        "legacy_region_id",
        "legacy_region_code",
        "legacy_region_name",
        "has_legacy_hierarchy",
    ).dropDuplicates()

    value_columns = []

    for column_name in municipalities.columns:

        if column_name != "source_file":
            value_columns.append(column_name)

    conflicts = (
        municipalities.select(*value_columns)
        .distinct()
        .groupBy("municipality_id")
        .count()
        .filter(F.col("count") > 1)
    )

    if has_rows(conflicts):
        raise ValueError(
            "Existem dados conflitantes para o mesmo município. Revise a Bronze antes de gerar a Silver."
        )

    # Mantém todas as origens de registros equivalentes, sem escolher uma ao acaso.
    municipalities = municipalities.groupBy(*value_columns).agg(
        F.sort_array(F.collect_set("source_file")).alias("source_files"),
    )

    # A coluna auxiliar permite manter state_code dentro de cada Parquet.
    write_parquet(municipalities, output_directory, "municipalities")

    return municipalities
