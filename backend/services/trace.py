"""Consulta os vínculos atuais e resolve relações simples pelas chaves e origens preservadas."""

from datetime import datetime, timezone

from backend.core.database import get_connection
from pipeline.shared import execution
from pipeline.shared.files import fingerprint


class TraceUnavailableError(ValueError):
    pass


class TraceStore:
    def __init__(self, connection_factory=None, schema_name="gold"):
        self.connection_factory = connection_factory or get_connection
        self.schema_name = schema_name

    def state(self):
        with self.connection_factory() as connection:

            if (
                connection.execute("SELECT to_regclass('pipeline_trace.current')").fetchone()[0]
                is None
            ):
                return None

            row = connection.execute(
                "SELECT payload FROM pipeline_trace.current WHERE singleton=true"
            ).fetchone()

        payload = row[0] if row else None

        if payload and payload.get("schema_name", "gold") != self.schema_name:
            return None

        return payload

    def links_many(self, node_id, records, direction):
        column = "output" if direction == "parents" else "input"
        identities = []

        for record in records:

            identities.append(record["record_id"])

        if not identities:
            return {}

        with self.connection_factory() as connection:

            rows = connection.execute(
                "SELECT output_id, output_node, output_key, input_id, input_node, input_key, relation FROM pipeline_trace.links WHERE "
                + column
                + "_node=%s AND "
                + column
                + "_id=ANY(%s)",
                (node_id, identities),
            ).fetchall()

        grouped = {}

        for output_id, output_node, output_key, input_id, input_node, input_key, relation in rows:

            origin = output_id if direction == "parents" else input_id
            target = input_node if direction == "parents" else output_node
            key = input_key if direction == "parents" else output_key
            grouped.setdefault(origin, []).append(
                {"node_id": target, "key": key, "relation": relation}
            )

        return grouped


class TraceService:
    def __init__(self, reader, store):
        self.reader = reader
        self.store = store

    def ensure_current(self, node_id, published):
        state = execution.current(self.reader.directory / "pipeline_status.json")
        self.reader.year = str(state.get("year", self.reader.year))

        if state["status"] in {"running", "failed"}:
            raise TraceUnavailableError(
                "Conclua a pipeline e o Load para consultar um caminho consistente dos dados atuais."
            )

        if not published or published["run_id"] != state["run_id"]:
            raise TraceUnavailableError(
                "Execute a Gold e o Load para publicar os vínculos dos dados atuais."
            )

        node = self.reader.node(node_id)

        if node["layer"] == "load":
            node = self.reader.node("gold:" + node["dataset"])

        if node["layer"] == "source":
            expected = published["files"].get(node["id"])

            if (
                expected is not None
                and fingerprint(
                    self.reader.directory / node["path"], {".csv", ".json", ".xls", ".xlsx"}
                )
                != expected
            ):
                raise TraceUnavailableError(
                    "Os arquivos originais mudaram após a publicação. Execute as camadas dependentes e o Load."
                )

            node = self.reader.node("bronze:" + node["dataset"])

        if fingerprint(self.reader.directory / node["path"]) != published["files"].get(node["id"]):
            raise TraceUnavailableError(
                "Os arquivos desta camada mudaram. Atualize a Gold e o Load antes de rastrear."
            )

    def direct(self, record, direction):
        node = self.reader.node(record["node_id"])
        row = record["values"]
        key = record["key"]
        dataset = node["dataset"]
        layer = node["layer"]
        references = []

        def add(target, target_key, relation="preserved_key"):
            references.append({"node_id": target, "key": target_key, "relation": relation})

        if direction == "parents":
            if layer == "source":
                return references

            if layer == "bronze":
                add("source:" + dataset, key, "original_file")
            elif layer == "load":
                add("gold:" + dataset, key, "published")
            elif layer == "silver":
                if dataset == "population" or dataset in {"inmet_daily", "inmet_boundary"}:
                    for origin in row["sources"]:

                        parent_key = {}

                        for field in self.reader.node("bronze:" + dataset)["key_fields"]:

                            parent_key[field] = origin[field]

                        add("bronze:" + dataset, parent_key, "source_row")
                else:
                    field = "id" if dataset == "municipalities" else "municipality_id"
                    add(
                        "bronze:" + dataset,
                        {field: row["municipality_id"]},
                        "daily_expansion" if dataset == "open_meteo_daily" else "normalized",
                    )
        else:
            if layer == "source":
                add("bronze:" + dataset, key, "ingested")
            elif layer == "bronze":
                if dataset in {"municipalities", "geocoding"}:
                    add(
                        "silver:" + dataset,
                        {"municipality_id": row.get("municipality_id", row.get("id"))},
                        "normalized",
                    )
                elif dataset == "open_meteo_daily":
                    return {
                        "query_node": "silver:" + dataset,
                        "filters": {"municipality_id": row["municipality_id"]},
                        "relation": "daily_expansion",
                    }
                elif dataset == "population":
                    return {
                        "query_node": "silver:population",
                        "source_contains": key,
                        "relation": "worksheet_row",
                    }
                else:
                    try:

                        day = row["observation_date"].replace("/", "-")
                        hour = row["observation_time_utc"].strip().split()[0]
                        timestamp = datetime.strptime(day + " " + hour, "%Y-%m-%d %H%M").replace(
                            tzinfo=timezone.utc
                        )
                        add(
                            "silver:" + dataset,
                            {
                                "station_code": row["station_code"].strip(),
                                "timestamp_utc": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                            },
                            "normalized_hour",
                        )
                    except (ValueError, KeyError):
                        return references
            elif layer == "gold":
                add("load:" + dataset, key, "published")

        return references
