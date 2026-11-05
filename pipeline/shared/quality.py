from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Variáveis e regras físicas compartilhadas entre as camadas.

def measurement_fields(
    source: str,
) -> list[str]:
    """Lista as variáveis cujo contrato de qualidade é compartilhado entre as camadas."""

    if source == "inmet":
        return [
            "precipitation_mm",
            "station_pressure_mb",
            "station_pressure_max_mb",
            "station_pressure_min_mb",
            "global_radiation_kj_m2",
            "air_temperature_c",
            "dew_point_temperature_c",
            "air_temperature_max_c",
            "air_temperature_min_c",
            "dew_point_temperature_max_c",
            "dew_point_temperature_min_c",
            "relative_humidity_max_percent",
            "relative_humidity_min_percent",
            "relative_humidity_percent",
            "wind_direction_degrees",
            "wind_gust_ms",
            "wind_speed_ms",
        ]

    if source == "open_meteo":
        return [
            "temperature_mean_c",
            "temperature_min_c",
            "temperature_max_c",
            "precipitation_sum_mm",
            "wind_speed_max_ms",
        ]

    raise ValueError("Fonte de qualidade desconhecida.")


def get_domain_rules(
    source: str,
) -> list:
    """Retorna regra, condição física e variáveis afetadas, sem classificar extremos estatísticos."""

    if source == "inmet":
        return [
                ("negative_precipitation", F.col("precipitation_mm") < 0, ["precipitation_mm"]),
                ("negative_radiation", F.col("global_radiation_kj_m2") < 0, ["global_radiation_kj_m2"]),
                ("negative_wind_speed", F.col("wind_speed_ms") < 0, ["wind_speed_ms"]),
                ("negative_wind_gust", F.col("wind_gust_ms") < 0, ["wind_gust_ms"]),
                (
                    "wind_direction_outside_range",
                    ~F.col("wind_direction_degrees").between(0, 360),
                    ["wind_direction_degrees"],
                ),
                (
                    "humidity_outside_range",
                    ~F.col("relative_humidity_percent").between(0, 100),
                    ["relative_humidity_percent"],
                ),
                (
                    "humidity_max_outside_range",
                    ~F.col("relative_humidity_max_percent").between(0, 100),
                    ["relative_humidity_max_percent"],
                ),
                (
                    "humidity_min_outside_range",
                    ~F.col("relative_humidity_min_percent").between(0, 100),
                    ["relative_humidity_min_percent"],
                ),
                ("nonpositive_pressure", F.col("station_pressure_mb") <= 0, ["station_pressure_mb"]),
                (
                    "nonpositive_pressure_max",
                    F.col("station_pressure_max_mb") <= 0,
                    ["station_pressure_max_mb"],
                ),
                (
                    "nonpositive_pressure_min",
                    F.col("station_pressure_min_mb") <= 0,
                    ["station_pressure_min_mb"],
                ),
                (
                    "temperature_max_below_min",
                    F.col("air_temperature_max_c") < F.col("air_temperature_min_c"),
                    ["air_temperature_max_c", "air_temperature_min_c"],
                ),
                (
                    "dewpoint_max_below_min",
                    F.col("dew_point_temperature_max_c") < F.col("dew_point_temperature_min_c"),
                    ["dew_point_temperature_max_c", "dew_point_temperature_min_c"],
                ),
                (
                    "pressure_max_below_min",
                    F.col("station_pressure_max_mb") < F.col("station_pressure_min_mb"),
                    ["station_pressure_max_mb", "station_pressure_min_mb"],
                ),
                (
                    "humidity_max_below_min",
                    F.col("relative_humidity_max_percent") < F.col("relative_humidity_min_percent"),
                    ["relative_humidity_max_percent", "relative_humidity_min_percent"],
                ),
            ]

    if source == "open_meteo":
        return [
                ("negative_precipitation", F.col("precipitation_sum_mm") < 0, ["precipitation_sum_mm"]),
                ("negative_wind_speed", F.col("wind_speed_max_ms") < 0, ["wind_speed_max_ms"]),
                (
                    "temperature_min_above_max",
                    F.col("temperature_min_c") > F.col("temperature_max_c"),
                    ["temperature_min_c", "temperature_max_c"],
                ),
                (
                    "temperature_mean_below_min",
                    F.col("temperature_mean_c") < F.col("temperature_min_c"),
                    ["temperature_mean_c", "temperature_min_c"],
                ),
                (
                    "temperature_mean_above_max",
                    F.col("temperature_mean_c") > F.col("temperature_max_c"),
                    ["temperature_mean_c", "temperature_max_c"],
                ),
            ]

    raise ValueError("Fonte de qualidade desconhecida.")


# Coerência entre medições, ausências, campos inválidos e alertas.

def validate_quality(
    dataframe: DataFrame,
    source: str,
) -> None:
    """Rejeita contradições entre valores, regras físicas, alertas e campos afetados."""

    fields = measurement_fields(source)
    rules = get_domain_rules(source)
    expected_fields = []
    allowed_flags = []
    invalid_conditions = {}
    contradictory = F.lit(False)

    for field in fields:
        expected_fields.append(F.lit(field))
        allowed_flags.append(F.lit("invalid_numeric:" + field))
        invalid_conditions[field] = F.array_contains("quality_flags", "invalid_numeric:" + field)

    for rule_name, condition, affected_fields in rules:
        active = F.coalesce(condition, F.lit(False))
        allowed_flags.append(F.lit(rule_name))
        contradictory = contradictory | ~F.array_contains("quality_flags", rule_name).eqNullSafe(active)

        for field in affected_fields:
            invalid_conditions[field] = invalid_conditions[field] | active

    for column_name in ["missing_fields", "invalid_fields", "quality_flags"]:
        contradictory = contradictory | F.col(column_name).isNull()

    for column_name in ["missing_fields", "invalid_fields"]:
        contradictory = contradictory | (F.size(F.array_except(column_name, F.array(*expected_fields))) > 0)

    contradictory = contradictory | (F.size(F.array_except("quality_flags", F.array(*allowed_flags))) > 0)

    for field in fields:
        value = F.col(field)
        numeric_flag = F.array_contains("quality_flags", "invalid_numeric:" + field)
        expected_missing = value.isNull() & ~numeric_flag
        contradictory = contradictory | ~F.array_contains("missing_fields", field).eqNullSafe(
            expected_missing
        )
        contradictory = contradictory | ~F.array_contains("invalid_fields", field).eqNullSafe(
            invalid_conditions[field]
        )
        # Números não interpretáveis já devem ter sido convertidos para NULL na Silver.
        contradictory = contradictory | (numeric_flag & value.isNotNull())
        contradictory = (
            contradictory | F.isnan(value) | F.coalesce(F.abs(value) == float("inf"), F.lit(False))
        )

    if has_rows(dataframe.filter(contradictory)):
        raise ValueError(
            "Valores, regras físicas e metadados de qualidade incoerentes na fonte "
            + source
            + ". Revise a Silver antes de gerar a Gold."
        )


def has_rows(dataframe: DataFrame) -> bool:
    """Verifica se uma condição de validação encontrou registros."""

    return dataframe.limit(1).count() > 0
