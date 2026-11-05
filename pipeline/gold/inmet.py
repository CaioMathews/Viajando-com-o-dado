from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from timezonefinder import TimezoneFinder

from pipeline.shared.lineage import write_inmet_links
from pipeline.shared.quality import has_rows
from pipeline.shared import execution
from pipeline.shared.storage import write_parquet
from pipeline.shared.capitals import validate_capitals
from pipeline.shared.quality import validate_quality


@execution.step("gold", "inmet")
def build_inmet(
    spark: SparkSession,
    input_directory: Path,
    capitals_directory: Path,
    output_directory: Path,
    start_date: date,
    end_date: date,
    max_distance_km: float = 50.0,
    minimum_station_coverage: float = 0.9,
    boundary_directory: Path | None = None,
) -> DataFrame:
    """Gera indicadores diários por capital com estação fixa, cobertura e rastreabilidade."""

    input_directory = input_directory.resolve()
    capitals_directory = capitals_directory.resolve()
    output_directory = output_directory.resolve()

    for source_directory in [input_directory, capitals_directory]:

        if (
            source_directory == output_directory
            or source_directory in output_directory.parents
            or output_directory in source_directory.parents
        ):
            raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    if start_date > end_date or max_distance_km <= 0 or not 0 < minimum_station_coverage <= 1:
        raise ValueError("Período, distância ou cobertura mínima inválidos.")

    capitals = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(capitals_directory.as_posix())
    )

    validate_capitals(spark, capitals)

    # Resolve o calendário da capital; não confunde o fuso da estação com o destino da análise.
    finder = TimezoneFinder(in_memory=False)
    timezone_records = []

    for capital in capitals.select("municipality_id", "latitude", "longitude").collect():

        timezone_name = finder.timezone_at(lat=capital["latitude"], lng=capital["longitude"])

        if timezone_name is None:
            raise ValueError(f"Fuso não identificado para a capital {capital['municipality_id']}.")

        timezone_records.append((capital["municipality_id"], timezone_name))

    timezones = spark.createDataFrame(timezone_records, "municipality_id long, timezone string")
    capitals = capitals.select(
        "municipality_id", "name", "state_code", "latitude", "longitude"
    ).join(timezones, "municipality_id")

    dates = spark.range(1).select(
        F.explode(F.sequence(F.lit(start_date), F.lit(end_date))).alias("observation_date")
    )
    calendar = capitals.crossJoin(dates)

    # Mede a duração real de cada dia local, incluindo dias de 23 ou 25 horas.
    local_start = F.col("observation_date").cast("timestamp_ntz")
    local_end = F.date_add("observation_date", 1).cast("timestamp_ntz")

    calendar = calendar.withColumn(
        "day_start_utc_clock", F.convert_timezone("timezone", F.lit("UTC"), local_start)
    )
    calendar = calendar.withColumn(
        "day_end_utc_clock", F.convert_timezone("timezone", F.lit("UTC"), local_end)
    )
    calendar = calendar.withColumn(
        "expected_hours",
        (F.expr("cast((day_end_utc_clock - day_start_utc_clock) AS BIGINT)") / 3600).cast("long"),
    )

    required_bounds = calendar.agg(
        F.min("day_start_utc_clock").alias("start"), F.max("day_end_utc_clock").alias("end")
    ).first()
    calendar = calendar.drop("day_start_utc_clock", "day_end_utc_clock")

    raw = (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(input_directory.as_posix())
        .withColumn("_lineage_node", F.lit("silver:inmet_daily"))
    )

    # As horas da borda completam dias locais de 2025, sem criar dias de análise em 2026.
    if boundary_directory is None:
        candidate_directory = input_directory.parent / f"boundary_{end_date.year}"

        if candidate_directory.is_dir():
            boundary_directory = candidate_directory

    boundary_status = None

    if boundary_directory is not None:
        boundary_directory = boundary_directory.resolve()

        if (
            boundary_directory == output_directory
            or boundary_directory in output_directory.parents
            or output_directory in boundary_directory.parents
        ):
            raise ValueError("A pasta de borda e a saída devem ser separadas.")

        boundary = (
            spark.read.option("recursiveFileLookup", True)
            .option("pathGlobFilter", "*.parquet")
            .parquet(boundary_directory.as_posix())
            .withColumn("_lineage_node", F.lit("silver:inmet_boundary"))
        )
        # Aceita somente a mesma estação e UF, com até 100 m de diferença cadastral.
        # Essa tolerância trata arredondamentos de coordenadas, não mudanças de local.
        canonical = raw.select(
            "station_code", "station_name", "state_code", "station_latitude", "station_longitude"
        ).distinct()
        combined = boundary.alias("b").join(canonical.alias("m"), "station_code", "left")

        latitude_delta = F.radians(F.col("b.station_latitude") - F.col("m.station_latitude"))
        longitude_delta = F.radians(F.col("b.station_longitude") - F.col("m.station_longitude"))

        separation = F.pow(F.sin(latitude_delta / 2), 2) + F.cos(
            F.radians(F.col("b.station_latitude"))
        ) * F.cos(F.radians(F.col("m.station_latitude"))) * F.pow(F.sin(longitude_delta / 2), 2)
        separation_m = (
            6371008.8 * 2 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), separation))))
        )

        compatible = (
            F.col("m.station_latitude").isNotNull()
            & F.col("b.state_code").eqNullSafe(F.col("m.state_code"))
            & (separation_m <= 100.0)
        )

        boundary_status = combined.groupBy("station_code").agg(
            F.max(F.when(compatible, 1).otherwise(0)).alias("compatible")
        )
        boundary_status = boundary_status.withColumn(
            "boundary_metadata_status",
            F.when(F.col("compatible") == 1, "compatible").otherwise("metadata_mismatch"),
        ).drop("compatible")
        boundary_columns = []

        for column_name in raw.columns:

            origin = "b"

            if column_name in [
                "station_name",
                "state_code",
                "station_latitude",
                "station_longitude",
            ]:
                origin = "m"

            boundary_columns.append(F.col(origin + "." + column_name).alias(column_name))

        usable_boundary = combined.filter(compatible).select(*boundary_columns)
        raw = raw.unionByName(usable_boundary)

    utc_clock_bounds = F.expr(
        "timestampadd(MICROSECOND, unix_micros(timestamp_utc), TIMESTAMP_NTZ '1970-01-01 00:00:00')"
    )
    available_bounds = (
        raw.select(utc_clock_bounds.alias("clock"))
        .agg(F.min("clock").alias("start"), F.max("clock").alias("end"))
        .first()
    )

    if (
        available_bounds["start"] is None
        or available_bounds["start"] > required_bounds["start"]
        or available_bounds["end"] < required_bounds["end"]
    ):
        raise ValueError(
            f"A entrada UTC não cobre integralmente os dias locais solicitados. Necessário de {required_bounds['start']} a {required_bounds['end']} UTC, incluindo o encerramento da última hora. Informe a pasta Silver de borda em boundary_directory."
        )

    validate_quality(raw, "inmet")

    variables = [
        "air_temperature_c",
        "air_temperature_min_c",
        "air_temperature_max_c",
        "precipitation_mm",
        "wind_speed_ms",
        "wind_gust_ms",
    ]
    columns = [
        F.col("station_code"),
        F.col("station_name"),
        F.col("state_code"),
        F.col("station_latitude"),
        F.col("station_longitude"),
        F.col("timestamp_utc"),
        F.col("sources"),
        F.col("_lineage_node"),
    ]

    for variable in variables:

        valid = (
            F.col(variable).isNotNull()
            & ~F.isnan(variable)
            & (F.abs(F.col(variable)) != float("inf"))
            & ~F.array_contains("invalid_fields", variable)
        )
        columns.append(F.when(valid, F.col(variable)).alias(variable))

    observations = raw.select(*columns).repartition(64).persist(StorageLevel.DISK_ONLY)
    selected_observations = None

    try:

        observations.count()
        stations = observations.select(
            "station_code", "station_name", "state_code", "station_latitude", "station_longitude"
        ).distinct()

        if has_rows(stations.groupBy("station_code").count().filter(F.col("count") > 1)):
            raise ValueError("Existem metadados divergentes para a mesma estação.")

        candidates = capitals.join(stations, "state_code", "inner")

        latitude_difference = F.radians(F.col("station_latitude") - F.col("latitude"))
        longitude_difference = F.radians(F.col("station_longitude") - F.col("longitude"))

        haversine = F.pow(F.sin(latitude_difference / 2), 2) + F.cos(F.radians("latitude")) * F.cos(
            F.radians("station_latitude")
        ) * F.pow(F.sin(longitude_difference / 2), 2)

        candidates = candidates.withColumn(
            "station_distance_km",
            6371.0088 * 2 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), haversine)))),
        )

        candidates = candidates.filter(F.col("station_distance_km") <= max_distance_km)

        # Associa cada observação ao calendário da capital para avaliar a cobertura do período.
        candidate_keys = candidates.select("municipality_id", "station_code", "timezone")

        evaluated = observations.join(F.broadcast(candidate_keys), "station_code", "inner")

        utc_clock = F.expr(
            "timestampadd(MICROSECOND, unix_micros(timestamp_utc), TIMESTAMP_NTZ '1970-01-01 00:00:00')"
        )

        evaluated = evaluated.withColumn("utc_clock", utc_clock)
        evaluated = evaluated.withColumn(
            "instant_date", F.to_date(F.convert_timezone(F.lit("UTC"), "timezone", "utc_clock"))
        )
        evaluated = evaluated.withColumn(
            "interval_date",
            F.to_date(
                F.convert_timezone(
                    F.lit("UTC"), "timezone", F.col("utc_clock") - F.expr("INTERVAL 1 MICROSECOND")
                )
            ),
        )

        temperature_valid = (
            F.col("instant_date").between(start_date, end_date)
            & F.col("air_temperature_c").isNotNull()
        )

        rain_valid = (
            F.col("interval_date").between(start_date, end_date)
            & F.col("precipitation_mm").isNotNull()
        )

        coverage = evaluated.groupBy("municipality_id", "station_code").agg(
            F.countDistinct(F.when(temperature_valid, F.col("timestamp_utc"))).alias(
                "period_temperature_hours"
            ),
            F.countDistinct(F.when(rain_valid, F.col("timestamp_utc"))).alias("period_rain_hours"),
        )

        expected = calendar.groupBy("municipality_id").agg(
            F.sum("expected_hours").alias("period_expected_hours")
        )

        ranked = candidates.join(coverage, ["municipality_id", "station_code"], "left").join(
            expected, "municipality_id"
        )
        ranked = ranked.withColumn(
            "station_temperature_coverage",
            F.coalesce(F.col("period_temperature_hours"), F.lit(0))
            / F.col("period_expected_hours"),
        )

        ranked = ranked.withColumn(
            "station_rain_coverage",
            F.coalesce(F.col("period_rain_hours"), F.lit(0)) / F.col("period_expected_hours"),
        )

        eligible = ranked.filter(
            (F.col("station_temperature_coverage") >= minimum_station_coverage)
            & (F.col("station_rain_coverage") >= minimum_station_coverage)
        )

        ordering = Window.partitionBy("municipality_id").orderBy(
            "station_distance_km",
            F.desc("station_temperature_coverage"),
            F.desc("station_rain_coverage"),
            "station_code",
        )

        selected = eligible.withColumn("station_rank", F.row_number().over(ordering)).filter(
            F.col("station_rank") == 1
        )
        selected = selected.select(
            "municipality_id",
            "station_code",
            "station_name",
            "station_latitude",
            "station_longitude",
            "station_distance_km",
            "station_temperature_coverage",
            "station_rain_coverage",
        )

        if boundary_status is not None:
            selected = selected.join(boundary_status, "station_code", "left")
            selected = selected.withColumn(
                "boundary_metadata_status",
                F.coalesce("boundary_metadata_status", F.lit("no_boundary_records")),
            )
        else:
            selected = selected.withColumn("boundary_metadata_status", F.lit("not_provided"))

        # A estação escolhida permanece fixa no período; não troca por outra em dias com falhas.
        selected_observations = evaluated.join(
            F.broadcast(selected.select("municipality_id", "station_code")),
            ["municipality_id", "station_code"],
            "inner",
        ).persist(StorageLevel.DISK_ONLY)

        if has_rows(
            selected_observations.groupBy("municipality_id", "station_code", "timestamp_utc")
            .count()
            .filter(F.col("count") > 1)
        ):
            raise ValueError("Existem observações repetidas para a estação e horário selecionados.")

        files = F.expr("transform(sources, origin -> origin.source_file)")

        instant = (
            selected_observations.filter(F.col("instant_date").between(start_date, end_date))
            .groupBy("municipality_id", F.col("instant_date").alias("observation_date"))
            .agg(
                F.avg("air_temperature_c").alias("temperature_mean_available_c"),
                F.count("air_temperature_c").alias("temperature_hours"),
                F.max("wind_speed_ms").alias("wind_max_available_ms"),
                F.count("wind_speed_ms").alias("wind_hours"),
                F.array_distinct(F.flatten(F.collect_list(files))).alias("instant_source_files"),
            )
        )

        intervals = (
            selected_observations.filter(F.col("interval_date").between(start_date, end_date))
            .groupBy("municipality_id", F.col("interval_date").alias("observation_date"))
            .agg(
                F.sum("precipitation_mm").alias("precipitation_available_mm"),
                F.count("precipitation_mm").alias("precipitation_hours"),
                F.max("wind_gust_ms").alias("gust_max_available_ms"),
                F.count("wind_gust_ms").alias("gust_hours"),
                F.min("air_temperature_min_c").alias("temperature_min_available_c"),
                F.count("air_temperature_min_c").alias("temperature_min_hours"),
                F.max("air_temperature_max_c").alias("temperature_max_available_c"),
                F.count("air_temperature_max_c").alias("temperature_max_hours"),
                F.array_distinct(F.flatten(F.collect_list(files))).alias("interval_source_files"),
            )
        )

        daily = (
            calendar.join(selected, "municipality_id", "left")
            .join(instant, ["municipality_id", "observation_date"], "left")
            .join(intervals, ["municipality_id", "observation_date"], "left")
        )

        metrics = [
            ("temperature", "temperature_mean_available_c", "temperature_mean_c", 0.9),
            ("precipitation", "precipitation_available_mm", "precipitation_sum_mm", 1.0),
            ("temperature_min", "temperature_min_available_c", "temperature_min_c", 1.0),
            ("temperature_max", "temperature_max_available_c", "temperature_max_c", 1.0),
            ("wind", "wind_max_available_ms", "wind_speed_max_ms", 1.0),
            ("gust", "gust_max_available_ms", "wind_gust_max_ms", 1.0),
        ]

        # Não inventa medições nem apresenta soma ou extremo parcial como indicador completo.
        for prefix, available, output_name, threshold in metrics:

            daily = daily.withColumn(
                prefix + "_hours", F.coalesce(F.col(prefix + "_hours"), F.lit(0))
            )
            daily = daily.withColumn(
                prefix + "_coverage", F.col(prefix + "_hours") / F.col("expected_hours")
            )
            daily = daily.withColumn(
                output_name, F.when(F.col(prefix + "_coverage") >= threshold, F.col(available))
            )
            daily = daily.drop(available)

        daily = daily.withColumn(
            "assignment_status",
            F.when(F.col("station_code").isNull(), "no_eligible_station").otherwise("assigned"),
        )
        daily = daily.withColumn("data_source", F.lit("inmet"))
        daily = daily.withColumn("selection_start_date", F.lit(start_date)).withColumn(
            "selection_end_date", F.lit(end_date)
        )
        daily = daily.withColumn("max_station_distance_km", F.lit(max_distance_km)).withColumn(
            "minimum_station_coverage", F.lit(minimum_station_coverage)
        )

        empty_files = F.array().cast("array<string>")

        daily = daily.withColumn(
            "source_files",
            F.sort_array(
                F.array_distinct(
                    F.concat(
                        F.coalesce("instant_source_files", empty_files),
                        F.coalesce("interval_source_files", empty_files),
                    )
                )
            ),
        )
        daily = daily.drop("instant_source_files", "interval_source_files")

        write_parquet(daily, output_directory, "inmet")
        write_inmet_links(selected_observations, output_directory, start_date, end_date)

    finally:
        observations.unpersist()

        if selected_observations is not None:
            selected_observations.unpersist()

    return (
        spark.read.option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.parquet")
        .parquet(output_directory.as_posix())
    )
