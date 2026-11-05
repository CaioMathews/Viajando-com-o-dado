"""Consulta os arquivos atuais das camadas; não lê arquivos de auditoria nem inicia Spark."""

from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from decimal import Decimal, InvalidOperation
from threading import Lock
import json
import os

import duckdb

from pipeline.shared.catalog import find_node
from pipeline.shared.files import fingerprint
from pipeline.shared.record_keys import clean, record_key, record_id


def identifier(name):
    return '"' + name.replace('"', '""') + '"'


class CurrentData:
    def __init__(self, directory, year="2025", gold=None):
        self.directory = Path(directory).resolve()
        self.year = str(year)
        self.gold = gold
        self._session = ContextVar("current_data_session", default=None)
        self._schemas = {}
        self._schema_lock = Lock()

    def node(self, node_id):
        return find_node(node_id, self.year)

    def paths(self, node):
        selected = node

        if node["layer"] in {"source", "load"}:
            selected = self.node(
                ("bronze:" if node["layer"] == "source" else "gold:") + node["dataset"]
            )

        directory = (self.directory / selected["path"]).resolve()

        if not directory.is_relative_to(self.directory):
            raise ValueError("Pasta de dados fora do projeto.")

        result = []
        pending = [directory]

        # scandir reaproveita os metadados da listagem. Não resolve de novo todos
        # os ancestrais de cada arquivo no compartilhamento Windows/Docker.
        while pending:

            current = pending.pop()

            if not current.exists():
                continue

            with os.scandir(current) as entries:

                for entry in entries:

                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    elif entry.name.endswith(".parquet") and entry.is_file():
                        path = Path(entry.path)

                        if entry.is_symlink() and not path.resolve().is_relative_to(self.directory):
                            raise ValueError("Arquivo fora da pasta de dados.")

                        result.append(str(path))

        result.sort()

        if not result:
            raise FileNotFoundError("A saída deste nó ainda não está disponível.")

        return result

    @contextmanager
    def session(self):
        """Reutiliza conexão e metadados somente durante esta consulta HTTP."""

        if self._session.get() is not None:
            yield

            return

        with self.connection() as connection:

            token = self._session.set({"connection": connection, "relations": {}})

            try:

                yield
            finally:
                self._session.reset(token)

    @contextmanager
    def connection(self):
        session = self._session.get()

        if session is not None:
            yield session["connection"]

            return

        with duckdb.connect(
            ":memory:", config={"threads": 1, "memory_limit": "1GB", "TimeZone": "UTC"}
        ) as connection:

            yield connection

    def relation(self, connection, node, alias="records"):
        session = self._session.get()
        node_id = node["id"]

        if node["layer"] == "source":
            node_id = "bronze:" + node["dataset"]
        elif node["layer"] == "load":
            node_id = "gold:" + node["dataset"]

        if session is not None:
            if node_id in session["relations"]:
                return session["relations"][node_id]

            alias = "node_" + node_id.replace(":", "_")

        paths = self.paths(node)
        connection.read_parquet(paths, union_by_name=True, hive_partitioning=False).create_view(
            alias
        )
        relation = identifier(alias)

        if session is not None:
            session["relations"][node_id] = relation

        return relation

    def columns(self, node_id):
        node = self.node(node_id)
        selected = (
            self.node("bronze:" + node["dataset"])
            if node["layer"] == "source"
            else self.node("gold:" + node["dataset"]) if node["layer"] == "load" else node
        )
        directory = (self.directory / selected["path"]).resolve()

        if not directory.is_relative_to(self.directory):
            raise ValueError("Pasta de dados fora do projeto.")

        signature = fingerprint(directory)

        with self._schema_lock:

            cached = self._schemas.get(str(directory))

            if cached is not None and cached[0] == signature:
                return dict(cached[1])

        with self.connection() as connection:

            relation = self.relation(connection, node)
            columns = {}

            for column in connection.execute("SELECT * FROM " + relation + " LIMIT 0").description:

                columns[column[0]] = str(column[1])

        # Guarda somente o schema, nunca resultados ou cópias dos dados. Alterações
        # nos arquivos invalidam a entrada antes da próxima pesquisa.
        with self._schema_lock:

            self._schemas[str(directory)] = (signature, dict(columns))

        return columns

    def get_many(self, node_id, keys):
        if not keys:
            return []

        node = self.node(node_id)
        unique = {}

        for key in keys:

            if set(key) != set(node["key_fields"]):
                raise ValueError("Chave incompatível com o nó.")

            unique[record_id(node_id, key)] = key

        if node["layer"] == "load" and self.gold:
            rows = self.gold.by_keys(node["dataset"], list(unique.values()))
        else:
            with self.connection() as connection:

                relation = self.relation(connection, node)
                types = {}

                for column in connection.execute(
                    "SELECT * FROM " + relation + " LIMIT 0"
                ).description:

                    types[column[0]] = str(column[1])

                fields = []
                names = []

                for field in node["key_fields"]:

                    fields.append(
                        "try_cast(json_extract_string(value, '$."
                        + field
                        + "') AS "
                        + types[field]
                        + ") AS "
                        + identifier(field)
                    )
                    names.append(identifier(field))

                query = (
                    "WITH wanted AS (SELECT "
                    + ", ".join(fields)
                    + " FROM json_each(?)) SELECT r.* FROM "
                    + relation
                    + " r SEMI JOIN wanted USING ("
                    + ", ".join(names)
                    + ")"
                )
                parameters = [json.dumps(clean(list(unique.values())), ensure_ascii=False)]
                predicates = []

                # Filtros simples permitem ao Parquet descartar grupos antes do
                # cruzamento das chaves; o SEMI JOIN ainda garante a chave exata.
                for field in node["key_fields"]:

                    values = {}

                    for key in unique.values():

                        values[key[field]] = None

                    if len(values) <= 128:
                        placeholders = ", ".join(["?"] * len(values))
                        predicates.append("r." + identifier(field) + " IN (" + placeholders + ")")
                        parameters.extend(values)

                if predicates:
                    query += " WHERE " + " AND ".join(predicates)

                result = connection.execute(query, parameters)
                columns = []

                for column in result.description:

                    columns.append(column[0])

                rows = []

                for values in result.fetchall():

                    rows.append(dict(zip(columns, values)))

        wrapped = []

        for row in rows:

            wrapped.append(self.wrap(node, row))

        return wrapped

    def metadata(self, node_id):
        node = self.node(node_id)

        if node["layer"] == "load" and self.gold:
            result = self.gold.metadata(node["dataset"])

            return {**node, "columns": result["columns"], "records": result["records"]}

        with self.connection() as connection:

            relation = self.relation(connection, node)
            columns = []

            for name, data_type, nullable, *_rest in connection.execute(
                "DESCRIBE SELECT * FROM " + relation
            ).fetchall():

                columns.append(
                    {
                        "name": name,
                        "type": data_type,
                        "nullable": nullable == "YES",
                        "key": name in node["key_fields"],
                    }
                )

            count = connection.execute("SELECT count(*) FROM " + relation).fetchone()[0]

        return {**node, "columns": columns, "records": count}

    def wrap(self, node, row):
        values = clean(row)
        key = record_key(node, values)

        return {
            "node_id": node["id"],
            "key": key,
            "record_id": record_id(node["id"], key),
            "values": values,
        }

    def _population_origins(self, connection, filters):
        relation = self.relation(connection, self.node("silver:population"), "population_origins")
        where = []
        parameters = []

        for field in ["municipality_id", "state_code", "reference_date"]:

            if filters.get(field) is not None:
                where.append(identifier(field) + " = ?")
                parameters.append(filters[field])

        rows = connection.execute(
            "SELECT sources FROM " + relation + (" WHERE " + " AND ".join(where) if where else ""),
            parameters,
        ).fetchall()
        keys = []

        for row in rows:

            keys.extend(row[0])

        return keys

    def _conditions(self, connection, node, columns, filters):
        conditions = []
        parameters = []
        municipality = filters.get("municipality_id")

        if municipality is not None:
            column = (
                "municipality_id"
                if "municipality_id" in columns
                else "id" if node["dataset"] == "municipalities" else None
            )

            if column:
                conditions.append(identifier(column) + " = ?")
                parameters.append(municipality)
            elif node["dataset"] in {"inmet_daily", "inmet_boundary"}:
                gold = self.relation(connection, self.node("gold:inmet_daily"), "assigned_stations")
                conditions.append(
                    "station_code IN (SELECT DISTINCT station_code FROM "
                    + gold
                    + " WHERE municipality_id = ? AND station_code IS NOT NULL)"
                )
                parameters.append(municipality)

        has_population_filter = False

        for field in ["municipality_id", "state_code", "reference_date"]:

            if filters.get(field) is not None:
                has_population_filter = True

        if (
            node["dataset"] == "population"
            and "municipality_id" not in columns
            and has_population_filter
        ):
            origins = self._population_origins(connection, filters)
            choices = []

            for origin in origins:

                choices.append("(source_file = ? AND source_sheet = ? AND source_row_number = ?)")
                parameters.extend(
                    [origin["source_file"], origin["source_sheet"], origin["source_row_number"]]
                )

            conditions.append("(" + " OR ".join(choices) + ")" if choices else "false")

        if filters.get("state_code") is not None and "state_code" in columns:
            conditions.append("state_code = ?")
            parameters.append(filters["state_code"])

        day = filters.get("observation_date") or filters.get("reference_date")

        if day:
            field = (
                "reference_date"
                if "reference_date" in columns
                else (
                    "observation_date"
                    if "observation_date" in columns
                    else "timestamp_utc" if "timestamp_utc" in columns else None
                )
            )

            if field:
                conditions.append(
                    "try_cast(replace(cast("
                    + identifier(field)
                    + " as varchar), '/', '-') as date) = cast(? as date)"
                )
                parameters.append(day)
            elif "start_date" in columns:
                conditions.append(
                    "cast(? as date) BETWEEN try_cast(start_date as date) AND try_cast(end_date as date)"
                )
                parameters.append(day)

        field = filters.get("field")

        if field:
            if field not in columns:
                raise ValueError("Coluna indisponível neste nó.")

            kind = columns[field]
            numeric = kind.startswith(
                ("BIGINT", "INTEGER", "SMALLINT", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT")
            )

            if numeric:
                try:

                    value = Decimal(filters["value"].replace(",", "."))
                except InvalidOperation:
                    raise ValueError("Informe um número válido para " + field) from None

                if not value.is_finite():
                    raise ValueError("O filtro numérico precisa ser finito.")

                conditions.append(
                    "cast(" + identifier(field) + " as DOUBLE) = try_cast(? as DOUBLE)"
                )
                parameters.append(str(value))
            else:
                conditions.append(
                    "strip_accents(lower(cast("
                    + identifier(field)
                    + " as varchar))) LIKE ? ESCAPE '\\'"
                )
                value = (
                    filters["value"].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                )
                parameters.append("%" + value + "%")
        elif filters.get("text"):
            choices = []

            for name, data_type in columns.items():

                if data_type in {"VARCHAR", "BIGINT", "INTEGER"} and not name.startswith("source_"):
                    choices.append(
                        "strip_accents(lower(cast("
                        + identifier(name)
                        + " as varchar))) LIKE ? ESCAPE '\\'"
                    )
                    value = (
                        filters["text"]
                        .replace("\\", "\\\\")
                        .replace("%", "\\%")
                        .replace("_", "\\_")
                    )
                    parameters.append("%" + value + "%")

            conditions.append("(" + " OR ".join(choices) + ")" if choices else "false")

        return conditions, parameters

    def records(
        self, node_id, filters=None, *, limit=10, offset=0, keys=None, source_contains=None
    ):
        if not 1 <= limit <= 100 or offset < 0:
            raise ValueError("Use limite de 1 a 100 e deslocamento não negativo.")

        node = self.node(node_id)
        filters = filters or {}

        if node["layer"] == "load" and self.gold:

            def published(parameters):
                supported = {}

                for name in ["municipality_id", "state_code", "reference_date"]:

                    if parameters.get(name) is not None:
                        supported[name] = parameters[name]

                if parameters.get("observation_date") and node["dataset"] in {
                    "open_meteo_daily",
                    "inmet_daily",
                }:
                    supported.update(
                        start_date=parameters["observation_date"],
                        end_date=parameters["observation_date"],
                    )

                return self.gold.records(
                    node["dataset"],
                    limit=limit,
                    offset=offset,
                    search_field=parameters.get("field"),
                    search_value=parameters.get("value"),
                    search_text=parameters.get("text"),
                    **supported,
                )

            if keys is not None:
                rows = []

                for key in keys:

                    if set(key) != set(node["key_fields"]):
                        raise ValueError("Chave incompatível com o nó.")

                    rows.extend(published(key)["items"])

                items = []

                for row in rows[:limit]:

                    items.append(self.wrap(node, row))

                return {
                    "items": items,
                    "has_more": len(rows) > limit,
                    "next_offset": None,
                    "offset": offset,
                    "limit": limit,
                }

            result = published(filters)
            items = []

            for row in result["items"]:

                items.append(self.wrap(node, row))

            return {**result, "items": items}

        with self.connection() as connection:

            relation = self.relation(connection, node)
            columns = {}

            for column in connection.execute("SELECT * FROM " + relation + " LIMIT 0").description:

                columns[column[0]] = str(column[1])

            conditions, parameters = self._conditions(connection, node, columns, filters)

            if keys is not None:
                alternatives = []

                for key in keys:

                    if set(key) != set(node["key_fields"]):
                        raise ValueError("Chave incompatível com o nó.")

                    fields = []

                    for name in node["key_fields"]:

                        fields.append(identifier(name) + " = ?")
                        parameters.append(key[name])

                    alternatives.append("(" + " AND ".join(fields) + ")")

                conditions.append(
                    "(" + " OR ".join(alternatives) + ")" if alternatives else "false"
                )

            if source_contains:
                parts = []

                for field in ["source_file", "source_sheet", "source_row_number"]:

                    if field in source_contains:
                        parts.append("origin." + identifier(field) + " = ?")
                        parameters.append(source_contains[field])

                conditions.append(
                    "EXISTS (SELECT 1 FROM unnest(sources) AS input(origin) WHERE "
                    + " AND ".join(parts)
                    + ")"
                )

            query = "SELECT * FROM " + relation

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            order = []

            for name in node["key_fields"]:

                order.append(identifier(name))

            query += " ORDER BY " + ", ".join(order) + " LIMIT ? OFFSET ?"
            rows = connection.execute(query, parameters + [limit + 1, offset]).fetchall()
            names = list(columns)
            items = []

            for values in rows[:limit]:

                items.append(self.wrap(node, dict(zip(names, values))))

        more = len(rows) > limit

        return {
            "items": items,
            "limit": limit,
            "offset": offset,
            "has_more": more,
            "next_offset": offset + limit if more else None,
        }

    def record(self, node_id, key):
        rows = self.records(node_id, limit=1, keys=[key])["items"]

        if not rows:
            raise FileNotFoundError("Registro não encontrado nos dados atuais.")

        return rows[0]

    def source_files(self, node_id):
        node = self.node(node_id)

        if node["layer"] != "source":
            raise ValueError("O nó não é uma fonte.")

        directory = (self.directory / node["path"]).resolve()

        if not directory.is_relative_to(self.directory):
            raise ValueError("Pasta de dados fora do projeto.")

        files = []

        if not directory.is_dir():
            return files

        pending = [directory]

        while pending:

            current = pending.pop()

            with os.scandir(current) as entries:

                for entry in entries:

                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    elif (
                        Path(entry.name).suffix.lower() in {".json", ".csv", ".xls", ".xlsx"}
                        and entry.is_file()
                    ):
                        path = Path(entry.path)

                        # O diretório já é canônico; somente links precisam de
                        # nova resolução para garantir que permaneçam no projeto.
                        if entry.is_symlink() and not path.resolve().is_relative_to(self.directory):
                            continue

                        files.append(
                            {
                                "name": entry.name,
                                "path": path.relative_to(self.directory).as_posix(),
                                "size": entry.stat().st_size,
                            }
                        )

        files.sort(key=lambda item: Path(item["path"]))

        return files
