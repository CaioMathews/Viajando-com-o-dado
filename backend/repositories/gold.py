"""Consultas às cinco tabelas publicadas, com filtros e ordenação pelas chaves."""

from datetime import date
from decimal import Decimal, InvalidOperation

from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from backend.core.database import get_connection


DATASETS = {
    "municipalities": {
        "title": "Municípios",
        "source": "ibge",
        "keys": ["municipality_id"],
        "date_column": None,
    },
    "population": {
        "title": "População",
        "source": "ibge",
        "keys": ["municipality_id", "reference_date"],
        "date_column": "reference_date",
    },
    "geocoding": {
        "title": "Coordenadas das capitais",
        "source": "open_meteo",
        "keys": ["municipality_id"],
        "date_column": None,
    },
    "open_meteo_daily": {
        "title": "Clima diário Open-Meteo",
        "source": "open_meteo",
        "keys": ["municipality_id", "observation_date"],
        "date_column": "observation_date",
    },
    "inmet_daily": {
        "title": "Clima diário INMET",
        "source": "inmet",
        "keys": ["municipality_id", "observation_date"],
        "date_column": "observation_date",
    },
}
UNITS = {
    "latitude": "°",
    "longitude": "°",
    "capital_latitude": "°",
    "capital_longitude": "°",
    "weather_latitude": "°",
    "weather_longitude": "°",
    "station_latitude": "°",
    "station_longitude": "°",
    "temperature_mean_c": "°C",
    "temperature_min_c": "°C",
    "temperature_max_c": "°C",
    "precipitation_sum_mm": "mm",
    "wind_speed_max_ms": "m/s",
    "wind_gust_max_ms": "m/s",
    "elevation_m": "m",
    "station_distance_km": "km",
    "max_station_distance_km": "km",
    "population_estimate": "habitantes",
    "utc_offset_seconds": "s",
}


class DatasetNotFoundError(LookupError):
    pass


class DatasetUnavailableError(RuntimeError):
    pass


class GoldRepository:
    def __init__(self, schema="gold", connection_factory=None):
        self.schema = schema
        self.connection_factory = connection_factory or get_connection

    def definition(self, name):

        if name not in DATASETS:
            raise DatasetNotFoundError("Fonte de dados não encontrada.")

        return {"name": name, **DATASETS[name], "join_key": "municipality_id"}

    def catalog(self):
        items = []

        for name in DATASETS:
            items.append(self.definition(name))

        return {"items": items}

    def exists(self, connection, name):
        relation = sql.Identifier(self.schema, name).as_string(connection)

        return connection.execute("SELECT to_regclass(%s)", (relation,)).fetchone()[0] is not None

    def require_table(self, connection, name):

        if not self.exists(connection, name):
            raise DatasetUnavailableError("Dados ainda não publicados. Execute a carga da pipeline.")

    def summary(self, connection, name):
        definition = self.definition(name)

        if not self.exists(connection, name):
            return {
                **definition,
                "available": False,
                "records": 0,
                "municipalities": 0,
                "period_start": None,
                "period_end": None,
            }

        date_column = definition["date_column"]
        start = sql.SQL("NULL")
        end = sql.SQL("NULL")

        if date_column is not None:
            start = sql.SQL("min({})").format(sql.Identifier(date_column))
            end = sql.SQL("max({})").format(sql.Identifier(date_column))

        row = connection.execute(
            sql.SQL("SELECT count(*), count(DISTINCT municipality_id), {}, {} FROM {}").format(
                start,
                end,
                sql.Identifier(self.schema, name),
            )
        ).fetchone()

        return {
            **definition,
            "available": True,
            "records": row[0],
            "municipalities": row[1],
            "period_start": row[2],
            "period_end": row[3],
        }

    def overview(self):
        items = []

        with self.connection_factory() as connection:

            for name in DATASETS:
                items.append(self.summary(connection, name))

        return {"datasets": items}

    def metadata(self, name):
        self.definition(name)

        with self.connection_factory() as connection:

            self.require_table(connection, name)
            result = self.summary(connection, name)
            rows = connection.execute(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull, "
                "EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid=a.attrelid "
                "AND c.contype='p' AND a.attnum=ANY(c.conkey)) "
                "FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 "
                "AND NOT a.attisdropped ORDER BY a.attnum",
                (sql.Identifier(self.schema, name).as_string(connection),),
            ).fetchall()
            columns = []

            for column, data_type, nullable, primary_key in rows:
                unit = UNITS.get(column)

                if column.endswith("_hours"):
                    unit = "h"
                elif column.endswith("_coverage"):
                    unit = "proporção (0–1)"

                columns.append(
                    {
                        "name": column,
                        "type": data_type,
                        "nullable": nullable,
                        "primary_key": primary_key,
                        "unit": unit,
                    }
                )

            years = []
            date_column = result["date_column"]

            if date_column is not None:
                rows = connection.execute(
                    sql.SQL(
                        "SELECT DISTINCT extract(year FROM {})::int FROM {} WHERE {} IS NOT NULL ORDER BY 1"
                    ).format(
                        sql.Identifier(date_column),
                        sql.Identifier(self.schema, name),
                        sql.Identifier(date_column),
                    )
                ).fetchall()

                for row in rows:
                    years.append(row[0])

        return {**result, "columns": columns, "years": years}

    def by_keys(self, name, keys):
        definition = self.definition(name)
        fields = []
        names = []

        for field in definition["keys"]:
            kind = "bigint" if field == "municipality_id" else "date"
            fields.append(sql.SQL("{} " + kind).format(sql.Identifier(field)))
            names.append(sql.Identifier(field))

        query = sql.SQL("SELECT t.* FROM {} t JOIN jsonb_to_recordset(%s) AS wanted ({}) USING ({}) ORDER BY {}").format(sql.Identifier(self.schema, name), sql.SQL(", ").join(fields), sql.SQL(", ").join(names), sql.SQL(", ").join(names))

        with self.connection_factory() as connection:

            self.require_table(connection, name)

            with connection.cursor(row_factory=dict_row) as cursor:

                cursor.execute(query, (Jsonb(keys),))

                return cursor.fetchall()

    def records(
        self,
        name,
        *,
        municipality_id=None,
        state_code=None,
        year=None,
        start_date=None,
        end_date=None,
        reference_date=None,
        limit=50,
        offset=0,
        search_field=None,
        search_value=None,
        search_text=None,
    ):
        definition = self.definition(name)
        date_column = definition["date_column"]

        if not 1 <= limit <= 200 or offset < 0:
            raise ValueError("Paginação inválida.")

        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        has_date_filter = False

        for value in [year, start_date, end_date, reference_date]:
            if value is not None:
                has_date_filter = True

        if date_column is None and has_date_filter:
            raise ValueError("Esta fonte não possui coluna de data para o filtro informado.")

        if reference_date is not None and date_column != "reference_date":
            raise ValueError("O filtro reference_date pertence à fonte de população.")

        conditions = []
        parameters = []

        for column, value in [("municipality_id", municipality_id), ("state_code", state_code)]:
            if value is not None:
                conditions.append(sql.SQL("{} = %s").format(sql.Identifier(column)))
                parameters.append(value)

        if year is not None:
            if not 1 <= year <= 9999:
                raise ValueError("Ano inválido.")

            conditions.append(sql.SQL("{} BETWEEN %s AND %s").format(sql.Identifier(date_column)))
            parameters.extend([date(year, 1, 1), date(year, 12, 31)])

        for operator, value in [(">=", start_date), ("<=", end_date), ("=", reference_date)]:
            if value is not None:
                conditions.append(sql.SQL("{} " + operator + " %s").format(sql.Identifier(date_column)))
                parameters.append(value)

        if search_field or search_text:
            available = self.metadata(name)["columns"]
            names = []

            for column in available:
                names.append(column["name"])

            if search_field:
                if search_field not in names:
                    raise ValueError("Coluna indisponível neste nó.")

                kind = None

                for column in available:
                    if column["name"] == search_field:
                        kind = column["type"]

                if kind in {"bigint", "integer", "smallint", "double precision", "real", "numeric"}:
                    try:

                        value = Decimal(search_value.replace(",", "."))
                    except InvalidOperation:
                        raise ValueError("Informe um número válido para " + search_field) from None

                    if not value.is_finite():
                        raise ValueError("O filtro numérico precisa ser finito.")

                    conditions.append(sql.SQL("{} = %s").format(sql.Identifier(search_field)))
                    parameters.append(value)
                else:
                    conditions.append(sql.SQL("translate(lower({}::text), 'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc') LIKE %s").format(sql.Identifier(search_field)))
                    parameters.append("%" + search_value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
            else:
                conditions.append(sql.SQL("translate(lower(to_jsonb(t)::text), 'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc') LIKE %s"))
                parameters.append("%" + search_text.replace("%", "\\%").replace("_", "\\_") + "%")

        query = sql.SQL("SELECT * FROM {} t").format(sql.Identifier(self.schema, name))

        if conditions:
            query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)

        order = []

        for key in definition["keys"]:
            column = sql.Identifier(key)

            # O mapa pede uma linha da capital: prioriza a estimativa mais
            # recente, preservando as referências anteriores para consulta.
            if name == "population" and key == "reference_date":
                column = sql.SQL("{} DESC").format(column)

            order.append(column)

        query += sql.SQL(" ORDER BY {} LIMIT %s OFFSET %s").format(sql.SQL(", ").join(order))

        with self.connection_factory() as connection:

            self.require_table(connection, name)

            with connection.cursor(row_factory=dict_row) as cursor:

                cursor.execute(query, parameters + [limit + 1, offset])
                rows = cursor.fetchall()

        has_more = len(rows) > limit

        return {
            "items": rows[:limit],
            "limit": limit,
            "offset": offset,
            "has_more": has_more,
            "next_offset": offset + limit if has_more else None,
        }
