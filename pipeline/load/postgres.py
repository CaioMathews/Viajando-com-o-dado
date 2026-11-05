import json
import math
import os
from pathlib import Path
import re
from uuid import uuid4

import psycopg
from dotenv import load_dotenv
from psycopg import sql
from psycopg.types.json import Jsonb
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructType,
)

from pipeline.shared import execution, lineage


TABLE_KEYS = {
    "municipalities": ["municipality_id"],
    "population": ["municipality_id", "reference_date"],
    "geocoding": ["municipality_id"],
    "open_meteo_daily": ["municipality_id", "observation_date"],
    "inmet_daily": ["municipality_id", "observation_date"],
}


def validate_identifier(name: str) -> None:
    """Restringe nomes de schemas e tabelas usados pelo carregador."""

    if not isinstance(name, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None:
        raise ValueError("Use identificadores em inglês, minúsculos, com até 63 caracteres.")

    if name.startswith("pg_") or name == "information_schema":
        raise ValueError("O carregador não utiliza schemas reservados do PostgreSQL.")


def load_environment() -> None:
    """Lê o .env da raiz, preservando variáveis já configuradas e a senha literal."""

    environment_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(environment_path, override=False, interpolate=False)


def get_connection(database_name: str | None = None, autocommit: bool = False) -> psycopg.Connection:
    """Abre a conexão com configurações do ambiente, sem exibir a senha."""

    load_environment()

    for variable in ["PGDATABASE", "PGUSER", "PGPASSWORD"]:

        if not os.environ.get(variable):
            raise ValueError(f"Defina a variável de ambiente {variable} antes da carga.")

    port = int(os.environ.get("PGPORT", "5432"))

    if not 1 <= port <= 65535:
        raise ValueError("PGPORT deve estar entre 1 e 65535.")

    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=port,
        dbname=database_name or os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        sslmode=os.environ.get("PGSSLMODE", "prefer"),
        connect_timeout=10,
        application_name="viajando_com_o_dado_load",
        autocommit=autocommit,
    )


def create_database() -> bool:
    """Cria o banco configurado quando ausente, sem alterar bancos já existentes."""

    load_environment()
    database_name = os.environ.get("PGDATABASE", "")
    validate_identifier(database_name)

    # CREATE DATABASE exige autocommit e uma conexão com um banco já existente.
    with get_connection(database_name="postgres", autocommit=True) as connection:

        with connection.cursor() as cursor:

            cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)", (database_name,))

            if cursor.fetchone()[0]:
                return False

            try:

                cursor.execute(
                    sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(
                        sql.Identifier(database_name),
                    )
                )
            except psycopg.errors.DuplicateDatabase:
                # Outra execução pode ter criado o mesmo banco após a verificação.
                return False

    return True


def get_column_types(dataframe: DataFrame) -> dict[str, str]:
    """Mapeia tipos da Gold para SQL; origens, listas e estruturas permanecem em JSONB."""

    columns = {}

    for field in dataframe.schema.fields:
        validate_identifier(field.name)
        data_type = field.dataType

        if isinstance(data_type, (ByteType, ShortType)):
            postgres_type = "smallint"

        elif isinstance(data_type, IntegerType):
            postgres_type = "integer"

        elif isinstance(data_type, LongType):
            postgres_type = "bigint"

        elif isinstance(data_type, FloatType):
            postgres_type = "real"

        elif isinstance(data_type, DoubleType):
            postgres_type = "double precision"

        elif isinstance(data_type, DecimalType):
            postgres_type = f"numeric({data_type.precision},{data_type.scale})"

        elif isinstance(data_type, BooleanType):
            postgres_type = "boolean"

        elif isinstance(data_type, DateType):
            postgres_type = "date"

        elif isinstance(data_type, StringType):
            postgres_type = "text"

        elif isinstance(data_type, (ArrayType, MapType, StructType)):
            postgres_type = "jsonb"
        else:
            raise ValueError(f"Tipo não suportado em {field.name}: {data_type.simpleString()}.")

        columns[field.name] = postgres_type

    if len(columns) != len(dataframe.columns):
        raise ValueError("A entrada contém nomes de colunas repetidos.")

    return columns


def validate_dataframe(dataframe: DataFrame, table_name: str) -> int:
    """Bloqueia cargas vazias e chaves ausentes, nulas ou repetidas."""

    if table_name not in TABLE_KEYS:
        raise ValueError("Tabela fora das cinco saídas previstas para a Gold.")

    missing_keys = set(TABLE_KEYS[table_name]) - set(dataframe.columns)

    if missing_keys:
        raise ValueError(f"Chaves ausentes na tabela {table_name}: {sorted(missing_keys)}.")

    invalid_key = F.lit(False)

    for name in TABLE_KEYS[table_name]:
        invalid_key = invalid_key | F.col(name).isNull()

    if dataframe.filter(invalid_key).limit(1).count() > 0:
        raise ValueError(f"A tabela {table_name} contém chaves nulas.")

    repeated = dataframe.groupBy(*TABLE_KEYS[table_name]).count().filter(F.col("count") > 1)

    if repeated.limit(1).count() > 0:
        raise ValueError(f"A tabela {table_name} contém chaves repetidas.")

    record_count = dataframe.count()

    if record_count == 0:
        raise ValueError(f"A tabela {table_name} está vazia.")

    return record_count


def prepare_table(cursor, schema_name: str, table_name: str, columns: dict[str, str]) -> None:
    """Cria a tabela e recusa alterações implícitas em uma estrutura já existente."""

    definitions = []

    for name, postgres_type in columns.items():
        definitions.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(postgres_type)))

    keys = []

    for name in TABLE_KEYS[table_name]:
        keys.append(sql.Identifier(name))

    definitions.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(keys)))

    if table_name != "municipalities":
        definitions.append(
            sql.SQL("FOREIGN KEY (municipality_id) REFERENCES {} (municipality_id)").format(
                sql.Identifier(schema_name, "municipalities"),
            )
        )

    table = sql.Identifier(schema_name, table_name)
    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(table, sql.SQL(", ").join(definitions))
    )

    # Mantém o estado anterior estável durante a captura e o upsert; consultas continuam permitidas.
    cursor.execute(sql.SQL("LOCK TABLE {} IN SHARE ROW EXCLUSIVE MODE").format(table))
    relation = table.as_string(cursor)
    cursor.execute(
        "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
        (relation,),
    )

    if cursor.fetchall() != list(columns.items()):
        raise ValueError(
            f"O schema de {schema_name}.{table_name} diverge da Gold; revise a migração da tabela."
        )

    cursor.execute(
        "SELECT array_agg(a.attname ORDER BY k.position) FROM pg_constraint c "
        "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(number, position) "
        "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.number "
        "WHERE c.conrelid = %s::regclass AND c.contype = 'p'",
        (relation,),
    )

    if cursor.fetchone()[0] != TABLE_KEYS[table_name]:
        raise ValueError(f"A chave primária de {table_name} diverge da configuração da carga.")


def load_table(
    connection: psycopg.Connection, dataframe: DataFrame, table_name: str, schema_name: str = "gold"
) -> int:
    """Carrega uma tabela dentro da transação do chamador; não confirma alterações isoladamente."""

    validate_identifier(schema_name)

    if connection.autocommit:
        raise ValueError("Use uma conexão com autocommit desativado para preservar a transação.")

    expected_count = validate_dataframe(dataframe, table_name)
    columns = get_column_types(dataframe)
    selected_columns = []

    for name, postgres_type in columns.items():
        column = F.col(name)

        if postgres_type == "jsonb":
            column = F.to_json(column, {"ignoreNullFields": "false"})

        selected_columns.append(column.alias(name))

    with connection.cursor() as cursor:

        prepare_table(cursor, schema_name, table_name, columns)

        table = sql.Identifier(schema_name, table_name)
        staging = sql.Identifier("load_" + uuid4().hex)

        cursor.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(staging, table))
        names = []
        updates = []
        keys = []

        for name in columns:
            names.append(sql.Identifier(name))

            if name not in TABLE_KEYS[table_name]:
                updates.append(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name)))

        for name in TABLE_KEYS[table_name]:
            keys.append(sql.Identifier(name))

        record_count = 0
        copy_names = list(names)
        input_frame = dataframe

        copy_query = sql.SQL("COPY {} ({}) FROM STDIN").format(staging, sql.SQL(", ").join(copy_names))

        # Itera por partição; não coleta toda a Gold na memória do Python.
        with cursor.copy(copy_query) as copy:

            for row in input_frame.select(*selected_columns).toLocalIterator():
                values = []

                for name, postgres_type in columns.items():
                    value = row[name]

                    if value is not None and postgres_type == "jsonb":
                        value = Jsonb(json.loads(value))

                    elif isinstance(value, float) and not math.isfinite(value):
                        raise ValueError(f"Número não finito em {table_name}.{name}.")

                    values.append(value)

                copy.write_row(values)
                record_count += 1

        if record_count != expected_count:
            raise ValueError(f"A quantidade lida mudou durante a carga de {table_name}.")

        conflict_action = sql.SQL("DO NOTHING")

        if updates:
            conflict_action = sql.SQL(
                "DO UPDATE SET {} WHERE to_jsonb(target) IS DISTINCT FROM to_jsonb(EXCLUDED)"
            ).format(sql.SQL(", ").join(updates))

        cursor.execute(
            sql.SQL("INSERT INTO {} AS target ({}) SELECT {} FROM {} ON CONFLICT ({}) {}").format(
                table,
                sql.SQL(", ").join(names),
                sql.SQL(", ").join(names),
                staging,
                sql.SQL(", ").join(keys),
                conflict_action,
            )
        )

        cursor.execute(sql.SQL("DROP TABLE {}").format(staging))

    return record_count


def read_gold_tables(spark: SparkSession, gold_directory: Path, year: str = "2025") -> dict[str, DataFrame]:
    """Localiza as cinco saídas, sem modificar os arquivos da Gold."""

    year = str(year)

    if re.fullmatch(r"[0-9]{4}", year) is None or not 1 <= int(year) <= 9999:
        raise ValueError("Informe o ano com quatro dígitos.")

    gold_directory = gold_directory.resolve()

    paths = {
        "municipalities": gold_directory / "ibge" / "municipalities",
        "population": gold_directory / "ibge" / "population",
        "geocoding": gold_directory / "open_meteo" / "geocoding",
        "open_meteo_daily": gold_directory / "open_meteo" / "weather" / year,
        "inmet_daily": gold_directory / "inmet" / year,
    }

    for path in paths.values():

        if not path.is_dir():
            raise FileNotFoundError(f"Saída da Gold não encontrada: {path}")

    tables = {}

    for name, path in paths.items():
        tables[name] = (
            spark.read.option("recursiveFileLookup", True)
            .option("pathGlobFilter", "*.parquet")
            .parquet(path.as_posix())
        )

    return tables


@execution.step("load", "postgres")
def load_gold(
    spark: SparkSession, gold_directory: Path, year: str = "2025", schema_name: str = "gold"
) -> dict[str, int]:
    """Publica as cinco fontes na mesma transação, mantendo outros períodos já carregados."""

    validate_identifier(schema_name)
    tables = read_gold_tables(spark, gold_directory, year)
    summary = {}

    data_directory = gold_directory.resolve().parent
    before = {}

    for node in lineage.catalog(year):
        if node["layer"] in {"bronze", "silver", "gold"}:
            before[node["id"]] = lineage.fingerprint(data_directory / node["path"])
        elif node["layer"] == "source":
            before[node["id"]] = lineage.fingerprint(data_directory / node["path"], {".csv", ".json", ".xls", ".xlsx"})

    with get_connection() as connection:

        with connection.cursor() as cursor:

            cursor.execute("SET LOCAL lock_timeout = '10s'")
            cursor.execute("SELECT pg_try_advisory_xact_lock(hashtextextended(current_database() || ':' || %s, 0))", (schema_name,))

            if not cursor.fetchone()[0]:
                raise RuntimeError("Outra carga está usando este schema do PostgreSQL.")

            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))

        for table_name, dataframe in tables.items():
            summary[table_name] = load_table(connection, dataframe, table_name, schema_name)

        lineage.publish(connection, data_directory, year, schema_name)

        for node in lineage.catalog(year):
            suffixes = {".csv", ".json", ".xls", ".xlsx"} if node["layer"] == "source" else None

            if node["id"] in before and before[node["id"]] != lineage.fingerprint(data_directory / node["path"], suffixes):
                raise RuntimeError("Os dados mudaram durante a publicação. A transação será desfeita.")

    return summary
