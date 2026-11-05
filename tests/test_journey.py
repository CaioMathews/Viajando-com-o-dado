"""Confere limites do percurso, junções reais e igualdade dos filtros numéricos."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from backend.repositories.current_data import CurrentData
from backend.services.discovery import DiscoveryService
from backend.services.journey import JourneyService
from backend.services.trace import TraceUnavailableError
from pipeline.shared.catalog import find_node
from pipeline.shared.record_keys import record_id


def record(node, municipality=4312617, day=None):
    key = {"municipality_id": municipality}

    if node.endswith("_daily"):
        key["observation_date"] = day or "2025-01-01"
    elif node.endswith("population"):
        key["reference_date"] = "2025-07-01"

    return {
        "node_id": node,
        "key": key,
        "record_id": record_id(node, key),
        "values": {"name": "Muitos Capões", **key},
    }


class FakeReader:
    directory = Path("unused")
    year = "2025"

    def __init__(self, records):
        self.rows = {}

        for row in records:

            self.rows[row["record_id"]] = row

    def node(self, node_id):
        return find_node(node_id)

    def record(self, node_id, key):
        return self.rows[record_id(node_id, key)]

    def get_many(self, node_id, keys):
        result = []

        for key in keys:

            row = self.rows.get(record_id(node_id, key))

            if row:
                result.append(row)

        return result


class FakeStore:
    def state(self):
        return {"run_id": "current"}

    def links_many(self, *args):
        return {}


class FakeTrace:
    def __init__(self, rows, edges):
        self.reader = FakeReader(rows)
        self.edges = edges
        self.store = FakeStore()

    def ensure_current(self, *args):
        pass

    def direct(self, current, direction):
        result = []

        for source, target in self.edges:

            origin, neighbor = (target, source) if direction == "parents" else (source, target)

            if origin["record_id"] == current["record_id"]:
                result.append(
                    {
                        "node_id": neighbor["node_id"],
                        "key": neighbor["key"],
                        "relation": (
                            "published" if neighbor["node_id"].startswith("load:") else "transform"
                        ),
                    }
                )

        return result


class JourneyTests(unittest.TestCase):
    def complete(self, trace, seed):
        with patch(
            "backend.services.journey.execution.current",
            return_value={"status": "succeeded", "run_id": "current"},
        ):

            return JourneyService(trace).complete(seed["node_id"], seed["key"])

    def test_joins_are_included_without_following_unrelated_dimension_outputs(self):
        silver = record("silver:open_meteo_daily")
        gold = record("gold:open_meteo_daily")
        loaded = record("load:open_meteo_daily")
        location = record("gold:geocoding")
        municipality = record("gold:municipalities")
        unrelated = record("gold:population")
        edges = [
            (silver, gold),
            (gold, loaded),
            (location, gold),
            (municipality, location),
            (municipality, unrelated),
        ]
        trace = FakeTrace([silver, gold, loaded, location, municipality, unrelated], edges)
        result = self.complete(trace, gold)
        ids = set()

        for node in result["nodes"]:

            ids.add(node["id"])

        self.assertTrue(result["complete"])
        self.assertEqual(len(result["edges"]), 4)
        self.assertEqual(len(ids), 5)
        self.assertNotIn("gold:population", ids)

    def test_non_capital_ends_in_silver_with_explanation(self):
        seed = record("silver:population")
        result = self.complete(FakeTrace([seed], []), seed)
        self.assertEqual(len(result["nodes"]), 1)
        self.assertIn("27 capitais", result["stops"][0]["reason"])

    def test_missing_parent_does_not_claim_complete_trace(self):
        seed = record("gold:population")
        missing = record("silver:population")

        with self.assertRaises(TraceUnavailableError):

            self.complete(FakeTrace([seed], [(missing, seed)]), seed)


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.reader = CurrentData(Path(temporary.name))
        directory = self.reader.directory / self.reader.node("silver:population")["path"]
        directory.mkdir(parents=True)

        with duckdb.connect() as connection:

            connection.execute(
                "COPY (SELECT 4312617 AS municipality_id, 'Muitos Capões' AS name, 'RS' AS state_code, DATE '2025-07-01' AS reference_date, 2937::BIGINT AS population_estimate UNION ALL SELECT 4312618, 'Outro', 'RS', DATE '2025-07-01', 12937) TO ? (FORMAT PARQUET)",
                [str(directory / "rows.parquet")],
            )

        self.service = DiscoveryService(self.reader)

    def test_non_capital_names_and_exact_numeric_population(self):
        for query in ["Muitos Capoes", "municipality_name=Muitos Capões", "population=2937"]:

            result = self.service.search(query)
            self.assertEqual(len(result["items"]), 1, query)
            self.assertEqual(result["items"][0]["values"]["population_estimate"], 2937)

        self.assertEqual(self.service.parse("RS")["filters"], {"state_code": "RS"})
        self.assertFalse(self.service.search("population=2937.1")["items"])

    def test_batch_keys_return_only_requested_records(self):
        result = self.reader.get_many(
            "silver:population", [{"municipality_id": 4312617, "reference_date": "2025-07-01"}]
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["values"]["name"], "Muitos Capões")

    def test_unknown_training_fields_do_not_invent_results(self):
        result = self.service.search("split=train")
        self.assertFalse(result["items"])

    def test_multiple_metric_filters_are_not_silently_overwritten(self):
        with self.assertRaises(ValueError):

            self.service.parse("population=2937; station_code=A925")


if __name__ == "__main__":
    unittest.main()
