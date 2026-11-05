"""Confere isolamento das consultas, chaves compostas e paginação após a otimização."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb

from backend.repositories.current_data import CurrentData
from backend.services.discovery import DiscoveryService


class SearchOptimizationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.reader = CurrentData(Path(temporary.name))
        self.directory = self.reader.directory / self.reader.node("silver:population")["path"]
        self.directory.mkdir(parents=True)
        self.write_population(2937)

    def write_population(self, population):
        with duckdb.connect() as connection:

            connection.execute(
                "COPY (SELECT 4312617::BIGINT AS municipality_id, 'Muitos Capões' AS name, 'RS' AS state_code, DATE '2025-07-01' AS reference_date, ?::BIGINT AS population_estimate) TO ? (FORMAT PARQUET)",
                [population, str(self.directory / "rows.parquet")],
            )

    def test_next_request_sees_replaced_files(self):
        with self.reader.session():

            first = self.reader.records("silver:population")["items"][0]
            self.assertEqual(first["values"]["population_estimate"], 2937)

        self.write_population(3000)

        with self.reader.session():

            second = self.reader.records("silver:population")["items"][0]
            self.assertEqual(second["values"]["population_estimate"], 3000)

    def test_nested_queries_keep_their_own_views(self):
        with self.reader.session():

            with self.reader.session():

                self.reader.columns("silver:population")

            result = DiscoveryService(self.reader).search("population=2937")
            self.assertEqual(len(result["items"]), 1)
            self.assertEqual(result["items"][0]["key"]["municipality_id"], 4312617)

    def test_metric_parse_does_not_read_municipality_files(self):
        service = DiscoveryService(self.reader)

        with patch.object(
            service, "municipalities", side_effect=AssertionError("Leitura desnecessária")
        ):

            self.assertEqual(
                service.parse("humidity_pct=41")["filters"],
                {"field": "relative_humidity_percent", "value": "41"},
            )

    def test_key_filters_do_not_mix_compound_keys(self):
        target = self.reader.directory / self.reader.node("silver:inmet_daily")["path"]
        target.mkdir(parents=True)

        with duckdb.connect() as connection:

            connection.execute(
                "COPY (SELECT * FROM (VALUES ('A101', TIMESTAMP '2025-01-01 00:00:00'), ('A101', TIMESTAMP '2025-01-01 01:00:00'), ('A102', TIMESTAMP '2025-01-01 00:00:00'), ('A102', TIMESTAMP '2025-01-01 01:00:00')) AS t(station_code,timestamp_utc)) TO ? (FORMAT PARQUET)",
                [str(target / "rows.parquet")],
            )

        with self.reader.session():

            rows = self.reader.get_many(
                "silver:inmet_daily",
                [
                    {"station_code": "A101", "timestamp_utc": "2025-01-01T00:00:00.000000Z"},
                    {"station_code": "A102", "timestamp_utc": "2025-01-01T01:00:00.000000Z"},
                ],
            )
            self.assertEqual(len(rows), 2)
            identities = set()

            for row in rows:

                identities.add((row["key"]["station_code"], row["key"]["timestamp_utc"]))

            self.assertEqual(
                identities,
                {("A101", "2025-01-01T00:00:00.000000Z"), ("A102", "2025-01-01T01:00:00.000000Z")},
            )

    def test_schema_cache_is_invalidated_when_a_file_changes(self):
        self.assertNotIn("new_field", self.reader.columns("silver:population"))

        with duckdb.connect() as connection:

            connection.execute(
                "COPY (SELECT 4312617::BIGINT AS municipality_id, 'new' AS new_field) TO ? (FORMAT PARQUET)",
                [str(self.directory / "rows.parquet")],
            )

        self.assertIn("new_field", self.reader.columns("silver:population"))
