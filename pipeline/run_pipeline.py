import argparse
from datetime import date
import logging
import os
from pathlib import Path
import subprocess
import sys
from time import perf_counter


PROJECT_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_DIRECTORY))

import psycopg

from pyspark.sql import SparkSession

from pipeline.bronze import geocoding as bronze_geocoding
from pipeline.bronze import ibge_municipalities as bronze_municipalities
from pipeline.bronze import ibge_population as bronze_population
from pipeline.bronze import inmet as bronze_inmet
from pipeline.bronze import open_meteo as bronze_open_meteo
from pipeline.silver import geocoding as silver_geocoding
from pipeline.silver import ibge_municipalities as silver_municipalities
from pipeline.silver import ibge_population as silver_population
from pipeline.silver import inmet as silver_inmet
from pipeline.silver import open_meteo as silver_open_meteo
from pipeline.gold import geocoding as gold_geocoding
from pipeline.gold import ibge_municipalities as gold_municipalities
from pipeline.gold import ibge_population as gold_population
from pipeline.gold import inmet as gold_inmet
from pipeline.gold import open_meteo as gold_open_meteo
from pipeline.load import postgres
from pipeline.shared.storage import recover_output
from pipeline.shared import execution
from pipeline.shared.collection_storage import recover_collections


STAGES = ["consume", "bronze", "silver", "gold", "load"]
LOGGER = logging.getLogger(__name__)


def execute_step(name, function, *arguments, **options):
    """Mostra o andamento sem ocultar exceções das funções de cada fonte."""

    started = perf_counter()
    LOGGER.info("Iniciando: %s", name)
    result = function(*arguments, **options)
    LOGGER.info("Concluído: %s (%.1f segundos)", name, perf_counter() - started)

    return result


def validate_inputs(project_directory: Path, year: str, selected_stages: list[str]) -> None:
    """Confere as entradas da primeira camada antes de iniciar consultas ou processamento."""

    data = project_directory / "data"
    jar = project_directory / "pipeline/bronze/lib/hadoop-bare-naked-local-fs-0.1.0.jar"

    if os.name == "nt" and selected_stages != ["consume"] and not jar.is_file():
        raise FileNotFoundError(f"JAR necessário não encontrado: {jar}")

    paths = []

    if "bronze" in selected_stages:
        recover_collections(data)
        population_files = list((data / "ibge/population").glob("*.xls"))

        if len(population_files) != 1:
            raise ValueError("Mantenha exatamente uma planilha .xls em data/ibge/population.")

        for path in [data / "inmet" / year, data / "inmet" / f"boundary_{year}"]:

            if not path.is_dir() or not any(path.glob("*.CSV")):
                raise FileNotFoundError(
                    f"CSVs de entrada não encontrados: {path}. A borda do INMET deve ser obtida antes da execução."
                )

        if "consume" not in selected_stages:

            for path in [
                data / "ibge/municipalities",
                data / "open_meteo/geocoding",
                data / "open_meteo/weather" / year,
            ]:

                if not path.is_dir() or not any(path.glob("*.json")):
                    raise FileNotFoundError(
                        f"JSONs de entrada não encontrados: {path}. Execute primeiro a coleta."
                    )

    elif "silver" in selected_stages:
        paths = layer_paths(data / "bronze", year, include_boundary=True)

    elif "gold" in selected_stages:
        paths = layer_paths(data / "silver", year, include_boundary=True)

    elif "load" in selected_stages:
        paths = layer_paths(data / "gold", year, include_boundary=False)

    for path in paths:

        # Recupera uma publicação anterior antes de consultar sua pasta final.
        recover_output(path)

        if not path.is_dir() or not any(path.rglob("*.parquet")):
            raise FileNotFoundError(f"Parquets da camada anterior não encontrados: {path}")


def layer_paths(directory: Path, year: str, include_boundary: bool) -> list[Path]:
    paths = [
        directory / "ibge/municipalities",
        directory / "ibge/population",
        directory / "open_meteo/geocoding",
        directory / "open_meteo/weather" / year,
        directory / "inmet" / year,
    ]

    if include_boundary:
        paths.append(directory / "inmet" / f"boundary_{year}")

    return paths


def create_spark(project_directory: Path) -> SparkSession:
    """Compartilha a mesma sessão entre as três camadas e a leitura da carga."""

    os.environ["PYSPARK_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName("Viajando com o dado - Pipeline")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.pyspark.python", sys.executable)
    )

    if os.name == "nt":
        jar = project_directory / "pipeline/bronze/lib/hadoop-bare-naked-local-fs-0.1.0.jar"
        builder = (
            builder.config("spark.driver.extraClassPath", str(jar))
            .config("spark.executor.extraClassPath", str(jar))
            .config(
                "spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


@execution.step("consume", "consume")
def run_consume(project_directory: Path, year: str) -> None:
    # Um processo separado evita confundir geocoding/open_meteo da coleta com os das camadas.
    script = project_directory / "pipeline/consume APIs/consume.py"
    environment = os.environ.copy()
    environment["VCD_TRACKING_ENABLED"] = "0"
    subprocess.run(
        [sys.executable, str(script), "--year", year],
        cwd=project_directory,
        check=True,
        env=environment,
    )


def run_bronze(spark: SparkSession, data: Path, year: str) -> None:
    tasks = [
        ("municípios", bronze_municipalities.ingest_municipalities, "ibge/municipalities"),
        ("população", bronze_population.ingest_population, "ibge/population"),
        ("geocoding", bronze_geocoding.ingest_geocoding, "open_meteo/geocoding"),
        ("Open-Meteo", bronze_open_meteo.ingest_open_meteo, f"open_meteo/weather/{year}"),
        ("INMET", bronze_inmet.ingest_inmet, f"inmet/{year}"),
        ("borda INMET", bronze_inmet.ingest_inmet, f"inmet/boundary_{year}"),
    ]

    for name, function, relative in tasks:

        execute_step(
            "Bronze - " + name, function, spark, data / relative, data / "bronze" / relative
        )


def run_silver(spark: SparkSession, data: Path, year: str) -> None:
    tasks = [
        ("municípios", silver_municipalities.transform_municipalities, "ibge/municipalities"),
        ("população", silver_population.transform_population, "ibge/population"),
        ("geocoding", silver_geocoding.transform_geocoding, "open_meteo/geocoding"),
        ("Open-Meteo", silver_open_meteo.transform_open_meteo, f"open_meteo/weather/{year}"),
        ("INMET", silver_inmet.transform_inmet, f"inmet/{year}"),
        ("borda INMET", silver_inmet.transform_inmet, f"inmet/boundary_{year}"),
    ]

    for name, function, relative in tasks:

        execute_step(
            "Silver - " + name,
            function,
            spark,
            data / "bronze" / relative,
            data / "silver" / relative,
        )


def run_gold(spark: SparkSession, data: Path, year: str) -> None:
    silver = data / "silver"
    gold = data / "gold"
    municipalities = gold / "ibge/municipalities"
    coordinates = gold / "open_meteo/geocoding"
    start_date = date(int(year), 1, 1)
    end_date = date(int(year), 12, 31)

    execute_step(
        "Gold - municípios",
        gold_municipalities.build_municipalities,
        spark,
        silver / "ibge/municipalities",
        municipalities,
    )

    execute_step(
        "Gold - população",
        gold_population.build_population,
        spark,
        silver / "ibge/population",
        municipalities,
        gold / "ibge/population",
    )

    execute_step(
        "Gold - geocoding",
        gold_geocoding.build_geocoding,
        spark,
        silver / "open_meteo/geocoding",
        municipalities,
        coordinates,
    )

    execute_step(
        "Gold - Open-Meteo",
        gold_open_meteo.build_open_meteo,
        spark,
        silver / "open_meteo/weather" / year,
        coordinates,
        gold / "open_meteo/weather" / year,
        start_date,
        end_date,
    )

    execute_step(
        "Gold - INMET",
        gold_inmet.build_inmet,
        spark,
        silver / "inmet" / year,
        coordinates,
        gold / "inmet" / year,
        start_date,
        end_date,
        max_distance_km=50.0,
        minimum_station_coverage=0.9,
        boundary_directory=silver / "inmet" / f"boundary_{year}",
    )


@execution.pipeline_execution
def run_pipeline(
    year: str = "2025",
    start_at: str = "consume",
    stop_after: str = "load",
    schema_name: str = "gold",
) -> dict[str, int]:
    """Executa o intervalo selecionado e interrompe etapas seguintes se ocorrer uma falha."""

    year = str(year)
    date.fromisoformat(f"{year}-01-01")

    if (
        start_at not in STAGES
        or stop_after not in STAGES
        or STAGES.index(start_at) > STAGES.index(stop_after)
    ):
        raise ValueError("Informe um intervalo de etapas válido.")

    stages = STAGES[STAGES.index(start_at) : STAGES.index(stop_after) + 1]

    validate_inputs(PROJECT_DIRECTORY, year, stages)

    if "load" in stages:
        postgres.validate_identifier(schema_name)
        # Testa as credenciais antes do processamento longo; não modifica o banco.

        with postgres.get_connection(database_name="postgres") as connection:

            connection.execute("SELECT 1")

    data = PROJECT_DIRECTORY / "data"
    spark = None
    processing_error = None
    summary = {}

    try:

        for stage in stages:

            if stage == "consume":
                execute_step("Coleta", run_consume, PROJECT_DIRECTORY, year)
                continue

            if spark is None:
                spark = create_spark(PROJECT_DIRECTORY)

            if stage == "bronze":
                run_bronze(spark, data, year)

            elif stage == "silver":
                run_silver(spark, data, year)

            elif stage == "gold":
                run_gold(spark, data, year)

            elif stage == "load":
                execute_step("Preparação do banco", postgres.create_database)
                summary = execute_step(
                    "Carga PostgreSQL",
                    postgres.load_gold,
                    spark,
                    data / "gold",
                    year=year,
                    schema_name=schema_name,
                )

                for table_name, count in summary.items():

                    LOGGER.info("%s.%s: %s registros processados", schema_name, table_name, count)
    except BaseException as error:
        processing_error = error

        raise

    finally:

        if spark is not None:

            try:

                spark.stop()

            except BaseException as cleanup_error:
                details = execution.safe_error(cleanup_error)

                LOGGER.warning("Falha ao encerrar o Spark: %s", details["message"])

                # A finalização não substitui a causa original; uma falha isolada continua visível.
                if processing_error is None:
                    raise

    LOGGER.info("Execução concluída: %s até %s; ano %s.", start_at, stop_after, year)

    return summary


def publication_exists(year, schema_name):
    """Na inicialização, uma publicação existente nunca é reprocessada."""

    try:

        with postgres.get_connection() as connection:

            if (
                connection.execute("SELECT to_regclass('pipeline_trace.current')").fetchone()[0]
                is None
            ):
                return False

            row = connection.execute(
                "SELECT payload FROM pipeline_trace.current WHERE singleton=true"
            ).fetchone()
    except psycopg.errors.InvalidCatalogName:
        return False

    published = row[0] if row else None

    if not published:
        return False

    if published.get("schema_name") != schema_name or published.get("year") != str(year):
        raise ValueError(
            "Já existe uma publicação com outro ano ou schema. Reprocesse manualmente para alterar essa configuração."
        )

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa a pipeline Viajando com o dado.")
    parser.add_argument("--year", default="2025", help="Ano meteorológico. Padrão: 2025.")
    parser.add_argument(
        "--start-at", choices=STAGES, default="consume", help="Primeira etapa. Padrão: consume."
    )
    parser.add_argument(
        "--stop-after", choices=STAGES, default="load", help="Última etapa. Padrão: load."
    )
    parser.add_argument("--schema", default="gold", help="Schema PostgreSQL. Padrão: gold.")

    parser.add_argument(
        "--if-needed",
        action="store_true",
        help="Inicializa somente se não houver publicação; reprocessamentos são manuais.",
    )
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    try:

        postgres.validate_identifier(arguments.schema)

        if (
            arguments.if_needed
            and arguments.stop_after == "load"
            and publication_exists(arguments.year, arguments.schema)
        ):
            LOGGER.info(
                "Publicação existente; iniciando os serviços sem reexecutar a pipeline. Para atualizar os dados, execute o comando manual do README."
            )

            return 0

        if arguments.if_needed and execution.current().get("status") in {"failed", "running"}:
            raise ValueError(
                "A inicialização anterior não concluiu. Execute a pipeline manualmente conforme o README; não haverá nova tentativa automática."
            )

        run_pipeline(arguments.year, arguments.start_at, arguments.stop_after, arguments.schema)
    except Exception:
        LOGGER.exception("Pipeline interrompida. As etapas seguintes não foram executadas.")

        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
