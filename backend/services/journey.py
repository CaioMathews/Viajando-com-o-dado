"""Percurso completo do registro, com relações reais e leitura agrupada por nó."""

from pipeline.shared import execution
from pipeline.shared.catalog import catalog
from pipeline.shared.record_keys import record_id
from backend.services.trace import TraceUnavailableError


class JourneyService:
    def __init__(self, trace, max_records=40000):
        self.trace = trace
        self.reader = trace.reader
        self.max_records = max_records

    def complete(self, node_id, key):
        published = self.trace.store.state()
        self.trace.ensure_current(node_id, published)
        seed = self.reader.record(node_id, key)
        records = {seed["record_id"]: seed}
        edges = {}
        verified = set()

        def verify(target):
            if target not in verified:
                self.trace.ensure_current(target, published)
                verified.add(target)

        def add_edge(origin, neighbor, direction):
            source, target = (neighbor, origin) if direction == "parents" else (origin, neighbor)
            identity = (source["record_id"], target["record_id"])
            edges[identity] = (
                source["node_id"],
                target["node_id"],
                neighbor.get("relation", "preserved_key"),
            )

        def walk(initial, direction):
            pending = initial
            visited = set()

            while pending:

                groups = {}

                for record in pending:

                    identity = record["record_id"]

                    if identity not in visited:
                        visited.add(identity)
                        groups.setdefault(record["node_id"], []).append(record)

                pending = []

                for current_node, items in groups.items():

                    references = []
                    stored = self.trace.store.links_many(current_node, items, direction)

                    for origin in items:

                        direct = self.trace.direct(origin, direction)

                        if isinstance(direct, dict):
                            verify(direct["query_node"])
                            offset = 0

                            while True:

                                page = self.reader.records(
                                    direct["query_node"],
                                    direct.get("filters"),
                                    source_contains=direct.get("source_contains"),
                                    limit=100,
                                    offset=offset,
                                )

                                for neighbor in page["items"]:

                                    references.append(
                                        (
                                            origin,
                                            {
                                                "node_id": neighbor["node_id"],
                                                "key": neighbor["key"],
                                                "relation": direct["relation"],
                                            },
                                            False,
                                        )
                                    )

                                if not page["has_more"]:
                                    break

                                offset = page["next_offset"]
                        else:
                            for link in direct:

                                references.append(
                                    (
                                        origin,
                                        link,
                                        direction == "parents" or link["relation"] == "published",
                                    )
                                )

                        for link in stored.get(origin["record_id"], []):

                            references.append((origin, link, True))

                    requested = {}

                    for origin, link, required in references:

                        target = link["node_id"]
                        verify(target)
                        identity = record_id(target, link["key"])

                        if identity not in records:
                            requested.setdefault(target, {})[identity] = link["key"]

                    resolved = dict(records)

                    for target, keys in requested.items():

                        for neighbor in self.reader.get_many(target, list(keys.values())):

                            resolved[neighbor["record_id"]] = neighbor

                        if len(resolved) > self.max_records:
                            raise ValueError(
                                "Este caminho contém mais de 40 mil registros. Acrescente data ou horário à pesquisa para acompanhar um recorte completo."
                            )

                    for origin, link, required in references:

                        neighbor = resolved.get(record_id(link["node_id"], link["key"]))

                        if neighbor is None:
                            if required:
                                raise TraceUnavailableError(
                                    "Há um vínculo sem registro correspondente. Execute novamente as camadas dependentes e o Load."
                                )

                            continue

                        if link["relation"] == "normalized_hour":
                            matched = False

                            for source in neighbor["values"].get("sources", []):

                                if (
                                    source.get("source_file") == origin["key"]["source_file"]
                                    and source.get("source_row_number")
                                    == origin["key"]["source_row_number"]
                                ):
                                    matched = True

                            if not matched:
                                continue

                        records[neighbor["record_id"]] = neighbor
                        linked = {**neighbor, "relation": link["relation"]}
                        add_edge(origin, linked, direction)

                        if neighbor["record_id"] not in visited:
                            pending.append(neighbor)

        # Segue os destinos do registro pesquisado. Depois inclui todas as entradas
        # desses destinos, sem expandir saídas de dimensões e puxar dados alheios.
        walk([seed], "children")
        walk(list(records.values()), "parents")

        state = execution.current(self.reader.directory / "pipeline_status.json")

        if state["run_id"] != published["run_id"] or state["status"] != "succeeded":
            raise TraceUnavailableError(
                "A execução mudou durante a consulta. Aguarde sua conclusão e pesquise novamente."
            )

        for target in verified:

            self.trace.ensure_current(target, published)

        nodes = {}

        for record in records.values():

            group = nodes.setdefault(
                record["node_id"], {"id": record["node_id"], "records_count": 0, "examples": []}
            )
            group["records_count"] += 1

            if len(group["examples"]) < 3:
                group["examples"].append(record)

        grouped_edges = {}

        for source, target, relation in edges.values():

            edge = grouped_edges.setdefault(
                (source, target),
                {"source": source, "target": target, "records_count": 0, "relations": set()},
            )
            edge["records_count"] += 1
            edge["relations"].add(relation)

        output_edges = []

        for edge in grouped_edges.values():

            output_edges.append({**edge, "relations": sorted(edge["relations"])})

        definitions = catalog(self.reader.year)
        order = {}

        for index, definition in enumerate(definitions):

            order[definition["id"]] = (definition["column"], definition["row"])

        output_nodes = sorted(nodes.values(), key=lambda item: order[item["id"]])
        stops = []
        outgoing = set()

        for edge in output_edges:

            outgoing.add(edge["source"])

        for node in output_nodes:

            if node["id"] not in outgoing:
                definition = self.reader.node(node["id"])
                reason = (
                    "Dados publicados no PostgreSQL."
                    if definition["layer"] == "load"
                    else "O registro não possui contribuição registrada nas etapas seguintes."
                )

                if node["id"] == "silver:population":
                    reason = "A Silver preserva os municípios da planilha. A Gold publica somente estimativas das 27 capitais; este registro não participou dessa saída."
                elif definition["layer"] == "silver" and definition["dataset"].startswith("inmet"):
                    reason = "Esta hora não foi utilizada nos dias publicados: a estação pode não ter sido selecionada ou a hora está fora do período local."

                stops.append({"node_id": node["id"], "reason": reason})

        return {
            "record": seed,
            "nodes": output_nodes,
            "edges": output_edges,
            "stops": stops,
            "records_count": len(records),
            "complete": True,
        }
