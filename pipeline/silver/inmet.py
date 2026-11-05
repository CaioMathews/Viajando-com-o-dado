from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from timezonefinder import TimezoneFinder

from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.inmet_header import validate_headers
from pipeline.shared.quality import get_domain_rules
from pipeline.shared.storage import write_parquet


@execution.step("silver", "inmet")
def transform_inmet(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Padroniza as observações horárias da Bronze e grava a Silver por UF."""

    input_directory = input_directory.resolve()
    output_directory = output_directory.resolve()

    # Impede que a gravação substitua a entrada ou uma pasta que a contenha.
    if (
        input_directory == output_directory
        or input_directory in output_directory.parents
        or output_directory in input_directory.parents
    ):
        raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    observations = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
    )

    # Protege também execuções que reutilizam uma Bronze já existente.
    validate_headers(observations)

    numeric_columns = [
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

    metadata_columns = [
        "station_region",
        "state_code",
        "station_name",
        "station_code",
        "station_founded_at",
    ]

    parsed_columns = [F.col("source_file"), F.col("source_row_number")]

    for column_name in metadata_columns:

        text = F.regexp_replace(F.trim(F.col(column_name)), r"\s+", " ")

        if column_name in ["station_region", "state_code", "station_code"]:
            text = F.upper(text)

        parsed_columns.append(text.alias(column_name))

    # Mantém a data de fundação como texto: anos de dois dígitos têm século ambíguo.
    for column_name in ["station_latitude", "station_longitude", "station_altitude"]:

        parsed_columns.append(
            F.expr(f"try_cast(translate(trim(`{column_name}`), ',', '.') AS DOUBLE)").alias(
                column_name
            )
        )

    parsed_columns.extend(
        [
            F.trim(F.col("observation_date")).alias("date_raw"),
            F.upper(F.trim(F.col("observation_time_utc"))).alias("time_raw"),
            F.trim(F.col("trailing_empty_field")).alias("trailing_empty_field"),
        ]
    )

    # Separa a conversão dos números de suas regras, evitando expressões gigantes no Spark.
    for column_name in numeric_columns:

        parsed_columns.append(
            F.expr(f"try_cast(translate(trim(`{column_name}`), ',', '.') AS DOUBLE)").alias(
                column_name
            )
        )
        parsed_columns.append(
            (F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")).alias(
                column_name + "_empty"
            )
        )

    # Partições menores limitam o volume relido de cada bloco em disco no modo local.
    parsed = observations.select(*parsed_columns).repartition(64).persist(StorageLevel.DISK_ONLY)
    clean = None
    grouped = None

    try:

        if parsed.count() == 0:
            raise ValueError("A entrada não contém observações para gerar a Silver.")

        identity_columns = metadata_columns + [
            "station_latitude",
            "station_longitude",
            "station_altitude",
            "source_file",
            "source_row_number",
        ]
        selected_columns = []
        missing_fields = []
        invalid_flags = []
        numeric_invalid_fields = []

        for column_name in identity_columns:

            selected_columns.append(F.col(column_name))

        for column_name in numeric_columns:

            value = F.col(column_name)
            missing = F.col(column_name + "_empty") | (value == -9999)
            invalid = ~F.col(column_name + "_empty") & (
                value.isNull() | F.isnan(value) | (F.abs(value) == F.lit(float("inf")))
            )
            selected_columns.append(F.when(~missing & ~invalid, value).alias(column_name))
            missing_fields.append(F.when(missing, F.lit(column_name)))
            invalid_flags.append(F.when(invalid, F.lit("invalid_numeric:" + column_name)))
            numeric_invalid_fields.append(F.when(invalid, F.lit(column_name)))

        normalized_date = F.regexp_replace(F.col("date_raw"), "/", "-")
        normalized_time = F.regexp_extract("time_raw", r"^([0-9]{4}) UTC$", 1)

        # O deslocamento explícito +0000 mantém o instante correto em qualquer fuso da sessão.
        timestamp = F.try_to_timestamp(
            F.concat(normalized_date, F.lit(" "), normalized_time, F.lit(" +0000")),
            F.lit("yyyy-MM-dd HHmm Z"),
        )

        selected_columns.extend(
            [
                F.expr("try_cast(replace(date_raw, '/', '-') AS DATE)").alias(
                    "observation_date_utc"
                ),
                timestamp.alias("timestamp_utc"),
                F.coalesce(
                    F.col("date_raw").rlike(r"^[0-9]{4}[-/][0-9]{2}[-/][0-9]{2}$")
                    & F.col("time_raw").rlike(r"^([01][0-9]|2[0-3])00 UTC$"),
                    F.lit(False),
                ).alias("valid_time_format"),
                F.col("trailing_empty_field"),
                F.array_compact(F.array(*missing_fields)).alias("missing_fields"),
                F.array_compact(F.array(*invalid_flags)).alias("invalid_flags"),
                F.array_compact(F.array(*numeric_invalid_fields)).alias("numeric_invalid_fields"),
            ]
        )

        clean = parsed.select(*selected_columns).persist(StorageLevel.DISK_ONLY)
        clean.count()
        parsed.unpersist()

        state_regions = {
            "AC": "N",
            "AM": "N",
            "AP": "N",
            "PA": "N",
            "RO": "N",
            "RR": "N",
            "TO": "N",
            "AL": "NE",
            "BA": "NE",
            "CE": "NE",
            "MA": "NE",
            "PB": "NE",
            "PE": "NE",
            "PI": "NE",
            "RN": "NE",
            "SE": "NE",
            "ES": "SE",
            "MG": "SE",
            "RJ": "SE",
            "SP": "SE",
            "PR": "S",
            "RS": "S",
            "SC": "S",
            "DF": "CO",
            "GO": "CO",
            "MS": "CO",
            "MT": "CO",
        }
        region_mapping = []

        for state_code, region_code in state_regions.items():

            region_mapping.extend([F.lit(state_code), F.lit(region_code)])

        expected_region = F.create_map(*region_mapping)[F.col("state_code")]
        invalid_identity = (
            F.col("station_code").isNull()
            | (F.col("station_code") == "")
            | F.col("station_name").isNull()
            | (F.col("station_name") == "")
            | expected_region.isNull()
            | ~F.col("station_region").eqNullSafe(expected_region)
            | F.col("station_latitude").isNull()
            | F.isnan("station_latitude")
            | ~F.col("station_latitude").between(-90, 90)
            | F.col("station_longitude").isNull()
            | F.isnan("station_longitude")
            | ~F.col("station_longitude").between(-180, 180)
            | F.col("source_file").isNull()
            | (F.trim(F.col("source_file")) == "")
            | F.col("source_row_number").isNull()
            | (F.col("source_row_number") < 10)
            | F.col("timestamp_utc").isNull()
            | F.col("observation_date_utc").isNull()
            | ~F.col("valid_time_format")
            | (F.length(F.coalesce(F.col("trailing_empty_field"), F.lit(""))) > 0)
        )

        if has_rows(clean.filter(invalid_identity)):
            raise ValueError(
                "Existem estações, coordenadas, datas, horários ou campos de origem inválidos. Revise a Bronze antes de gerar a Silver."
            )

        location_conflicts = (
            clean.groupBy("station_code")
            .agg(
                F.countDistinct(
                    F.struct("state_code", "station_latitude", "station_longitude")
                ).alias("locations"),
            )
            .filter(F.col("locations") > 1)
        )

        if has_rows(location_conflicts):
            raise ValueError(
                "Existem localizações conflitantes para a mesma estação. Revise a Bronze antes de gerar a Silver."
            )

        # Resolve o fuso somente no catálogo de estações, sem UDF sobre milhões de linhas.
        timezone_finder = TimezoneFinder(in_memory=False)
        station_timezones = []
        station_locations = clean.select(
            "station_code", "station_latitude", "station_longitude"
        ).distinct()

        for station in station_locations.collect():

            timezone_name = timezone_finder.timezone_at(
                lng=station["station_longitude"], lat=station["station_latitude"]
            )

            if timezone_name is None:
                raise ValueError(f"Fuso não identificado para a estação {station['station_code']}.")

            station_timezones.append(
                (
                    station["station_code"],
                    station["station_latitude"],
                    station["station_longitude"],
                    timezone_name,
                )
            )

        timezone_catalog = spark.createDataFrame(
            station_timezones,
            "station_code string, station_latitude double, station_longitude double, timezone string",
        )

        localized = clean.join(
            F.broadcast(timezone_catalog),
            ["station_code", "station_latitude", "station_longitude"],
            "inner",
        )

        # NTZ representa o relógio local; timestamp_utc continua sendo o instante original.
        # Reconstrói o relógio UTC pelo epoch para não perder horas repetidas no fim do horário de verão.
        utc_clock = F.expr(
            "timestampadd(MICROSECOND, unix_micros(timestamp_utc), TIMESTAMP_NTZ '1970-01-01 00:00:00')"
        )
        localized = localized.withColumn("timestamp_utc_clock", utc_clock)
        localized = localized.withColumn(
            "timestamp_local",
            F.convert_timezone(F.lit("UTC"), F.col("timezone"), "timestamp_utc_clock"),
        )
        localized = localized.withColumn("observation_date_local", F.to_date("timestamp_local"))
        localized = localized.withColumn(
            "utc_offset_seconds", F.expr("cast((timestamp_local - timestamp_utc_clock) AS BIGINT)")
        )

        # A chuva do INMET corresponde à hora anterior ao horário da mensagem.
        localized = localized.withColumn(
            "interval_start_utc", F.col("timestamp_utc") - F.expr("INTERVAL 1 HOUR")
        )
        localized = localized.withColumn("interval_end_utc", F.col("timestamp_utc"))
        localized = localized.withColumn(
            "interval_start_local",
            F.convert_timezone(
                F.lit("UTC"),
                F.col("timezone"),
                F.col("timestamp_utc_clock") - F.expr("INTERVAL 1 HOUR"),
            ),
        )
        localized = localized.withColumn("interval_end_local", F.col("timestamp_local"))
        # Subtrai no relógio UTC antes de converter, inclusive em saltos do horário de verão.
        localized = localized.withColumn(
            "interval_date_local",
            F.to_date(
                F.convert_timezone(
                    F.lit("UTC"),
                    F.col("timezone"),
                    F.col("timestamp_utc_clock") - F.expr("INTERVAL 1 MICROSECOND"),
                )
            ),
        )
        localized = localized.drop("timestamp_utc_clock")

        # Não descarta extremos ou alertas físicos; preserva os valores e identifica as regras.
        domain_rules = get_domain_rules("inmet")
        domain_flags = []
        domain_invalid_fields = []

        for rule_name, condition, affected_fields in domain_rules:

            domain_flags.append(F.when(condition, F.lit(rule_name)))

            for column_name in affected_fields:

                domain_invalid_fields.append(F.when(condition, F.lit(column_name)))

        prepared = (
            localized.withColumn(
                "quality_flags",
                F.concat("invalid_flags", F.array_compact(F.array(*domain_flags))),
            )
            .withColumn(
                "invalid_fields",
                F.array_distinct(
                    F.concat(
                        "numeric_invalid_fields", F.array_compact(F.array(*domain_invalid_fields))
                    )
                ),
            )
            .withColumn("data_source", F.lit("inmet"))
            .drop(
                "valid_time_format",
                "trailing_empty_field",
                "invalid_flags",
                "numeric_invalid_fields",
            )
        )

        value_columns = []

        for column_name in prepared.columns:

            if column_name not in [
                "station_code",
                "timestamp_utc",
                "source_file",
                "source_row_number",
            ]:
                value_columns.append(column_name)

        # Mínimo e máximo iguais indicam que todas as versões da chave têm o mesmo conteúdo.
        grouped = (
            prepared.repartition(64, "station_code", "timestamp_utc")
            .groupBy("station_code", "timestamp_utc")
            .agg(
                F.min(F.struct(*value_columns)).alias("minimum_values"),
                F.max(F.struct(*value_columns)).alias("maximum_values"),
                F.sort_array(F.collect_set(F.struct("source_file", "source_row_number"))).alias(
                    "sources"
                ),
            )
            .persist(StorageLevel.DISK_ONLY)
        )

        if has_rows(grouped.filter(~F.col("minimum_values").eqNullSafe(F.col("maximum_values")))):
            raise ValueError(
                "Existem medições conflitantes para a mesma estação e horário. Revise a Bronze antes de gerar a Silver."
            )

        # Reúne as origens das duplicatas equivalentes, sem escolher uma linha arbitrária.
        observations = grouped.select(
            "station_code", "timestamp_utc", "minimum_values.*", "sources"
        )
        write_parquet(observations, output_directory, "inmet")

    finally:
        parsed.unpersist()

        if clean is not None:
            clean.unpersist()

        if grouped is not None:
            grouped.unpersist()

    # Retorna a Silver gravada para não repetir o processamento das milhões de linhas.
    return (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(output_directory.as_posix())
    )
