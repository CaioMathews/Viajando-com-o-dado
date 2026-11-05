from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.quality import get_domain_rules
from pipeline.shared.storage import write_parquet


@execution.step("silver", "open_meteo")
def transform_open_meteo(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Expande as listas diárias da Bronze e grava o clima na Silver por UF."""

    input_directory = input_directory.resolve()
    output_directory = output_directory.resolve()

    # Impede que a gravação substitua a entrada ou uma pasta que a contenha.
    if (
        input_directory == output_directory
        or input_directory in output_directory.parents
        or output_directory in input_directory.parents
    ):
        raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    weather = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
    )

    if weather.limit(1).count() == 0:
        raise ValueError("A entrada não contém respostas para gerar a Silver.")

    variables = {
        "temperature_2m_mean": "temperature_mean_c",
        "temperature_2m_min": "temperature_min_c",
        "temperature_2m_max": "temperature_max_c",
        "precipitation_sum": "precipitation_sum_mm",
        "wind_speed_10m_max": "wind_speed_max_ms",
    }
    expected_units = {
        "time": "iso8601",
        "temperature_2m_mean": "°C",
        "temperature_2m_min": "°C",
        "temperature_2m_max": "°C",
        "precipitation_sum": "mm",
        "wind_speed_10m_max": "km/h",
    }

    invalid_units = F.lit(False)

    # Confere as unidades de entrada antes de padronizar o vento de km/h para m/s.
    for variable, unit in expected_units.items():

        invalid_units = invalid_units | ~F.col("weather.daily_units")[variable].eqNullSafe(
            F.lit(unit)
        )

    if has_rows(weather.filter(invalid_units)):
        raise ValueError(
            "Existem unidades ausentes ou diferentes das esperadas. Revise a Bronze antes de gerar a Silver."
        )

    series_columns = [
        F.col("municipality_id"),
        F.regexp_replace(F.trim(F.col("name")), r"\s+", " ").alias("name"),
        F.upper(F.trim(F.col("state_code"))).alias("state_code"),
        F.col("source_file"),
        F.col("start_date").alias("start_date_raw"),
        F.col("end_date").alias("end_date_raw"),
        F.expr("try_cast(start_date AS DATE)").alias("period_start_date"),
        F.expr("try_cast(end_date AS DATE)").alias("period_end_date"),
        F.col("weather.latitude").alias("weather_latitude"),
        F.col("weather.longitude").alias("weather_longitude"),
        F.col("weather.elevation").alias("elevation_m"),
        F.trim(F.col("weather.timezone")).alias("timezone"),
        F.col("weather.utc_offset_seconds"),
        F.col("weather.daily.time").alias("dates"),
    ]

    for variable in variables:

        series_columns.append(F.col("weather.daily." + variable).alias(variable))

    series = weather.select(*series_columns)
    series = series.withColumn(
        "expected_days", F.datediff("period_end_date", "period_start_date") + 1
    )

    state_ids = {
        "RO": "11",
        "AC": "12",
        "AM": "13",
        "RR": "14",
        "PA": "15",
        "AP": "16",
        "TO": "17",
        "MA": "21",
        "PI": "22",
        "CE": "23",
        "RN": "24",
        "PB": "25",
        "PE": "26",
        "AL": "27",
        "SE": "28",
        "BA": "29",
        "MG": "31",
        "ES": "32",
        "RJ": "33",
        "SP": "35",
        "PR": "41",
        "SC": "42",
        "RS": "43",
        "MS": "50",
        "MT": "51",
        "GO": "52",
        "DF": "53",
    }

    state_mapping = []

    for state_code, state_id in state_ids.items():

        state_mapping.extend([F.lit(state_code), F.lit(state_id)])

    expected_prefix = F.create_map(*state_mapping)[F.col("state_code")]
    invalid_metadata = (
        F.col("municipality_id").isNull()
        | ~F.col("municipality_id").between(1000000, 9999999)
        | expected_prefix.isNull()
        | ~F.substring(F.col("municipality_id").cast("string"), 1, 2).eqNullSafe(expected_prefix)
        | F.col("name").isNull()
        | (F.col("name") == "")
        | F.col("source_file").isNull()
        | (F.trim(F.col("source_file")) == "")
        | F.col("period_start_date").isNull()
        | F.col("period_end_date").isNull()
        | ~F.col("start_date_raw").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
        | ~F.col("end_date_raw").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
        | (F.col("expected_days") <= 0)
        | F.col("weather_latitude").isNull()
        | F.isnan("weather_latitude")
        | ~F.col("weather_latitude").between(-90, 90)
        | F.col("weather_longitude").isNull()
        | F.isnan("weather_longitude")
        | ~F.col("weather_longitude").between(-180, 180)
        | F.col("timezone").isNull()
        | (F.col("timezone") == "")
        | F.col("utc_offset_seconds").isNull()
        | ~F.col("utc_offset_seconds").between(-64800, 64800)
    )

    if has_rows(series.filter(invalid_metadata)):
        raise ValueError(
            "Existem identificadores, períodos, coordenadas ou fusos inválidos. Revise a Bronze antes de gerar a Silver."
        )

    array_names = ["dates"] + list(variables)
    invalid_arrays = F.size(F.array_distinct("dates")) != F.col("expected_days")

    # Valida todas as listas antes da expansão para não ocultar truncamentos ou desalinhamentos.
    for column_name in array_names:

        invalid_arrays = (
            invalid_arrays
            | F.col(column_name).isNull()
            | (F.size(column_name) != F.col("expected_days"))
        )

    if has_rows(series.filter(invalid_arrays)):
        raise ValueError(
            "Existem listas ausentes, tamanhos divergentes ou datas repetidas no período declarado."
        )

    daily = (
        series.select(
            "municipality_id",
            "name",
            "state_code",
            "source_file",
            "period_start_date",
            "period_end_date",
            "weather_latitude",
            "weather_longitude",
            "elevation_m",
            "timezone",
            "utc_offset_seconds",
            F.posexplode(F.arrays_zip(*array_names)).alias("source_day_index", "daily_record"),
        )
        .select("*", "daily_record.*")
        .drop("daily_record")
    )

    # As datas diárias pertencem ao calendário local da resposta; não são instantes UTC.
    daily = daily.withColumn("observation_date", F.expr("try_cast(dates AS DATE)"))
    invalid_dates = daily.filter(
        F.col("observation_date").isNull()
        | ~F.col("dates").rlike(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
        | (F.col("observation_date") < F.col("period_start_date"))
        | (F.col("observation_date") > F.col("period_end_date"))
    )

    if has_rows(invalid_dates):
        raise ValueError("Existem datas diárias inválidas ou fora do período declarado.")

    missing_fields = []
    invalid_flags = []
    numeric_invalid_fields = []
    numeric_values = []

    # Zero de chuva continua válido. A Open-Meteo não usa o marcador -9999 do INMET.
    for variable, output_name in variables.items():

        value = F.col(variable)
        invalid = F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
        # Padroniza o vento em m/s, mantendo a validação sobre o valor original.
        standardized_value = value

        if variable == "wind_speed_10m_max":
            standardized_value = value / 3.6

        numeric_values.append(F.when(~invalid, standardized_value).alias(output_name))
        missing_fields.append(F.when(value.isNull(), F.lit(output_name)))
        invalid_flags.append(F.when(invalid, F.lit("invalid_numeric:" + output_name)))
        numeric_invalid_fields.append(F.when(invalid, F.lit(output_name)))

    daily = daily.select(
        "municipality_id",
        "name",
        "state_code",
        "observation_date",
        "weather_latitude",
        "weather_longitude",
        "elevation_m",
        "timezone",
        "utc_offset_seconds",
        "source_file",
        "source_day_index",
        "period_start_date",
        "period_end_date",
        *numeric_values,
        F.array_compact(F.array(*missing_fields)).alias("missing_fields"),
        F.array_compact(F.array(*invalid_flags)).alias("invalid_flags"),
        F.array_compact(F.array(*numeric_invalid_fields)).alias("numeric_invalid_fields"),
    )

    # Preserva valores extremos e sinaliza inconsistências físicas para investigação.
    domain_rules = get_domain_rules("open_meteo")
    domain_flags = []
    domain_invalid_fields = []

    for rule_name, condition, affected_fields in domain_rules:

        domain_flags.append(F.when(condition, F.lit(rule_name)))

        for column_name in affected_fields:

            domain_invalid_fields.append(F.when(condition, F.lit(column_name)))

    daily = (
        daily.withColumn(
            "quality_flags",
            F.concat("invalid_flags", F.array_compact(F.array(*domain_flags))),
        )
        .withColumn(
            "invalid_fields",
            F.array_distinct(
                F.concat("numeric_invalid_fields", F.array_compact(F.array(*domain_invalid_fields)))
            ),
        )
        .withColumn("data_source", F.lit("open_meteo"))
        .drop("invalid_flags", "numeric_invalid_fields")
    )
    value_columns = []

    for column_name in daily.columns:

        if column_name not in [
            "source_file",
            "source_day_index",
            "period_start_date",
            "period_end_date",
        ]:
            value_columns.append(column_name)

    conflicts = (
        daily.select(*value_columns)
        .distinct()
        .groupBy("municipality_id", "observation_date")
        .count()
        .filter(F.col("count") > 1)
    )

    if has_rows(conflicts):
        raise ValueError(
            "Existem dados conflitantes para o mesmo município e dia. Revise a Bronze antes de gerar a Silver."
        )

    # Respostas sobrepostas podem repetir um dia: reúne todas as origens equivalentes.
    daily = daily.groupBy(*value_columns).agg(
        F.sort_array(
            F.collect_set(
                F.struct("source_file", "source_day_index", "period_start_date", "period_end_date")
            )
        ).alias("sources"),
    )
    write_parquet(daily, output_directory, "weather")

    return daily
