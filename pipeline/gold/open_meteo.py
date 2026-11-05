from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet
from pipeline.shared.capitals import validate_capitals
from pipeline.shared.quality import validate_quality


@execution.step("gold", "open_meteo")
def build_open_meteo(spark: SparkSession, input_directory: Path, capitals_directory: Path,
                     output_directory: Path, start_date: date, end_date: date,) -> DataFrame:
    """Prepara o clima diário das capitais, preservando o calendário e a qualidade por variável."""

    input_directory = input_directory.resolve()
    capitals_directory = capitals_directory.resolve()
    output_directory = output_directory.resolve()

    for source_directory in [input_directory, capitals_directory]:

        if source_directory == output_directory or source_directory in output_directory.parents or output_directory in source_directory.parents:
            raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    if start_date > end_date:
        raise ValueError("A data inicial deve ser anterior ou igual à data final.")

    capitals = (
        spark.read
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(capitals_directory.as_posix())
        .select("municipality_id", "name", "state_code", F.col("latitude").alias("capital_latitude"), F.col("longitude").alias("capital_longitude"))
    )

    validate_capitals(spark, capitals)

    weather = (
        spark.read
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
        .join(capitals.select("municipality_id"), "municipality_id", "inner")
    )

    if has_rows(weather.filter(F.col("observation_date").isNull())):
        raise ValueError("Existem datas ausentes na Silver Open-Meteo.")

    weather = weather.filter(F.col("observation_date").between(start_date, end_date))
    duplicates = weather.groupBy("municipality_id", "observation_date").count().filter(F.col("count") > 1)

    if has_rows(duplicates):
        raise ValueError("Existem registros repetidos para a mesma capital e data. Revise a Silver antes de gerar a Gold.")

    identities = weather.select(
        "municipality_id", F.col("name").alias("weather_name"), F.col("state_code").alias("weather_state_code"),
    ).distinct().join(capitals, "municipality_id")

    if has_rows(identities.filter(~F.col("weather_name").eqNullSafe(F.col("name")) | ~F.col("weather_state_code").eqNullSafe(F.col("state_code")))):
        raise ValueError("Existem divergências de nome ou UF entre a Open-Meteo e o cadastro das capitais.")

    invalid_metadata = (
        ~F.col("data_source").eqNullSafe(F.lit("open_meteo"))
        | F.col("missing_fields").isNull() | F.col("invalid_fields").isNull() | F.col("quality_flags").isNull()
        | F.col("sources").isNull() | (F.size("sources") == 0)
        | F.col("timezone").isNull() | (F.trim(F.col("timezone")) == "")
    )

    if has_rows(weather.filter(invalid_metadata)):
        raise ValueError("Existem metadados de fonte, qualidade, fuso ou origem inválidos na Silver.")

    variables = ["temperature_mean_c", "temperature_min_c", "temperature_max_c", "precipitation_sum_mm", "wind_speed_max_ms"]
    validate_quality(weather, "open_meteo")

    selected_columns = [F.col("municipality_id"), F.col("observation_date")]

    for column_name in ["weather_latitude", "weather_longitude", "elevation_m", "timezone", "utc_offset_seconds", "missing_fields", "invalid_fields", "quality_flags", "sources"]:
        selected_columns.append(F.col(column_name))

    for column_name in variables:
        selected_columns.append(F.col(column_name))

    # A resposta já é diária: presença de um valor não comprova cobertura horária de 100%.
    weather = weather.select(*selected_columns).withColumn("source_record_present", F.lit(True))

    dates = spark.range(1).select(F.explode(F.sequence(F.lit(start_date), F.lit(end_date))).alias("observation_date"))
    daily = capitals.crossJoin(dates).join(weather, ["municipality_id", "observation_date"], "left")
    daily = daily.withColumn("source_record_present", F.coalesce("source_record_present", F.lit(False)))

    empty_strings = F.array().cast("array<string>")
    all_variables = []

    for column_name in variables:
        all_variables.append(F.lit(column_name))

    daily = daily.withColumn("missing_fields", F.when(~F.col("source_record_present"), F.array(*all_variables)).otherwise(F.col("missing_fields")))
    daily = daily.withColumn("invalid_fields", F.coalesce("invalid_fields", empty_strings))
    daily = daily.withColumn("quality_flags", F.coalesce("quality_flags", empty_strings))
    daily = daily.withColumn("sources", F.coalesce("sources", F.array().cast(weather.schema["sources"].dataType)))

    # Invalida somente a variável afetada; o valor interpretável original continua na Silver.
    for column_name in variables:
        valid = (
            F.col(column_name).isNotNull() & ~F.isnan(column_name)
            & (F.abs(F.col(column_name)) != float("inf"))
            & ~F.array_contains("invalid_fields", column_name)
        )

        daily = daily.withColumn(column_name + "_available", F.coalesce(valid, F.lit(False)))
        daily = daily.withColumn(column_name, F.when(F.col(column_name + "_available"), F.col(column_name)))

    daily = daily.withColumn(
        "record_status",
        F.when(~F.col("source_record_present"), "missing_record")
        .when(F.size("invalid_fields") > 0, "has_invalid_values")
        .when(F.size("missing_fields") > 0, "has_missing_values")
        .when(~(F.col("temperature_mean_c_available") & F.col("temperature_min_c_available") & F.col("temperature_max_c_available") & F.col("precipitation_sum_mm_available") & F.col("wind_speed_max_ms_available")), "has_unavailable_values")
        .otherwise("complete"),
    )

    daily = daily.withColumn("data_source", F.lit("open_meteo"))
    daily = daily.withColumn("temporal_resolution", F.lit("daily"))
    daily = daily.withColumn("selection_start_date", F.lit(start_date)).withColumn("selection_end_date", F.lit(end_date))

    # Registra registros gerados pelo calendário sem atribuir seus valores a um pai incorreto.
    write_parquet(daily, output_directory, "weather")

    return daily
