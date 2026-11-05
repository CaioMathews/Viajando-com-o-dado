"""Pesquisa entre camadas, incluindo registros que não chegam à Gold."""

from threading import Lock

from backend.repositories.gold import DatasetUnavailableError
from backend.services.search import resolve, suggestions
from pipeline.shared.catalog import SOURCES
from pipeline.shared.files import fingerprint


ALIASES = {
    "population": "population_estimate",
    "humidity_pct": "relative_humidity_percent",
    "municipality_name": "name",
}


class DiscoveryService:
    def __init__(self, reader):
        self.reader = reader
        self._signature = None
        self._municipalities = []
        self._lock = Lock()

    def municipalities(self):
        node = self.reader.node("silver:population")
        signature = fingerprint(self.reader.directory / node["path"])

        with self._lock:

            if signature != self._signature:
                if signature:
                    with self.reader.connection() as connection:

                        table = self.reader.relation(connection, node)
                        rows = connection.execute(
                            "SELECT DISTINCT municipality_id, name, state_code FROM "
                            + table
                            + " ORDER BY municipality_id"
                        ).fetchall()

                    items = []

                    for municipality, name, state in rows:

                        items.append(
                            {"municipality_id": municipality, "name": name, "state_code": state}
                        )
                else:
                    items = (
                        self.reader.gold.records("geocoding", limit=200)["items"]
                        if self.reader.gold
                        else []
                    )

                self._municipalities = items
                self._signature = signature

            return list(self._municipalities)

    def suggest(self, query):
        return suggestions(query, self.municipalities())

    def parse(self, query):
        # Uma medição isolada não precisa carregar o cadastro de municípios.
        field, separator, value = query.partition("=")
        names = {"name", "capital", "municipality_name", "cidade", "uf", "state_code"}

        if ";" not in query and (
            query.strip().isdecimal() or separator and field.strip().casefold() not in names
        ):
            parsed = resolve(query, [])
            selected = parsed["filters"].get("field")

            if selected in ALIASES:
                parsed["filters"]["field"] = ALIASES[selected]

            return parsed

        municipalities = self.municipalities()
        states = set()

        for item in municipalities:

            states.add(item["state_code"])

        if query.strip().upper() in states:
            return {
                "filters": {"state_code": query.strip().upper()},
                "suggestions": [],
                "message": "",
            }

        parsed = resolve(query, municipalities)
        field = parsed["filters"].get("field")

        if field in ALIASES:
            parsed["filters"]["field"] = ALIASES[field]

        return parsed

    def preferred(self, dataset, filters, limit=1, offset=0):
        if "reference_date" in filters and dataset != "population":
            return None, filters, None

        if ("observation_date" in filters or "reference_date" in filters) and dataset in {
            "municipalities",
            "geocoding",
        }:
            return None, filters, None

        for layer in ["gold", "silver", "bronze"]:

            if dataset == "inmet_boundary" and layer == "gold":
                continue

            node_id = layer + ":" + dataset

            try:

                # A Gold preserva dias sem estação. Já as horas só pertencem a
                # um município pela estação associada; sem ela, não há vínculo.
                if (
                    layer != "gold"
                    and filters.get("municipality_id") is not None
                    and dataset in {"inmet_daily", "inmet_boundary"}
                ):
                    assigned = self.reader.records(
                        "gold:inmet_daily", {"municipality_id": filters["municipality_id"]}, limit=1
                    )

                    if not assigned["items"] or not assigned["items"][0]["values"].get(
                        "station_code"
                    ):
                        return None, filters, None

                field = filters.get("field")

                if field and field not in self.reader.columns(node_id):
                    continue

                chosen = dict(filters)

                if dataset == "population" and "observation_date" in chosen:
                    chosen["reference_date"] = chosen.pop("observation_date")

                page = self.reader.records(node_id, chosen, limit=limit)

                if page["items"]:
                    if offset:
                        page = self.reader.records(node_id, chosen, limit=limit, offset=offset)

                    return node_id, chosen, page
            except FileNotFoundError:
                continue

        return None, filters, None

    def search(self, query, limit=20, dataset_index=0, offset=0):
        parsed = self.parse(query)

        if parsed["suggestions"]:
            return {**parsed, "items": [], "has_more": False, "next_cursor": None}

        datasets = []

        for definition in SOURCES:

            datasets.append(definition[0])

        items = []
        positions = []
        field = parsed["filters"].get("field")
        population_only = False

        # As outras fontes municipais usam apenas capitais. Com a referência
        # completa publicada, um município externo a ela só pode ter população.
        municipality = parsed["filters"].get("municipality_id")

        if municipality is not None and self.reader.gold is not None:
            try:

                capitals = self.reader.gold.records("municipalities", limit=100)["items"]
                identities = set()

                for capital in capitals:

                    identities.add(capital["municipality_id"])

                population_only = len(identities) == 27 and municipality not in identities
            except DatasetUnavailableError:
                pass

        for index in range(dataset_index, len(datasets)):

            dataset = datasets[index]

            if population_only and dataset != "population":
                continue

            start = offset if index == dataset_index else 0
            node_id, filters, page = self.preferred(
                dataset, parsed["filters"], limit=limit + 1, offset=start
            )

            if node_id is None:
                continue

            for row_index, record in enumerate(page["items"]):

                items.append(record)
                positions.append({"dataset_index": index, "offset": start + row_index})

            if len(items) > limit:
                break

        message = parsed["message"]

        if not items and field:
            message = "Nenhum registro correspondente nas camadas atuais. Campos de agregados mensais ou treino só podem ser pesquisados se existirem neste projeto."

        return {
            "items": items[:limit],
            "message": message,
            "suggestions": [],
            "has_more": len(items) > limit,
            "next_cursor": positions[limit] if len(items) > limit else None,
        }
