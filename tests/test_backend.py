import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

import duckdb
from fastapi.testclient import TestClient
import psycopg
from psycopg import sql

from backend.core.config import Settings
from backend.core.database import get_connection
from backend.main import create_app
from backend.repositories.gold import GoldRepository


class BackendContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "audit"
        self.client = TestClient(create_app(self.root, cors_origins=["http://localhost:5173"]))
        self.addCleanup(self.client.close)

    def test_catalog_openapi_and_health_do_not_connect_to_database(self):

        with patch(
            "backend.core.database.psycopg.connect", side_effect=AssertionError("Sem banco na inicialização")
        ):

            self.assertEqual(self.client.get("/health").status_code, 200)
            response = self.client.get("/datasets")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(response.json()["items"]), 5)
            specification = self.client.get("/openapi.json").json()
            self.assertIn("/datasets/{dataset}/records", specification["paths"])
            self.assertIn("/pipeline/journey", specification["paths"])
            self.assertNotIn("/audit/runs", specification["paths"])

    def test_unknown_dataset_is_rejected_before_connection(self):

        with patch(
            "backend.core.database.psycopg.connect", side_effect=AssertionError("Sem consulta arbitrária")
        ):

            self.assertEqual(self.client.get("/datasets/unknown/records").status_code, 404)
            self.assertEqual(self.client.get("/datasets/unknown").status_code, 404)

    def test_invalid_filters_and_pagination_are_rejected_before_connection(self):

        with patch("backend.core.database.psycopg.connect", side_effect=AssertionError("Filtro inválido")):

            for query in [
                "limit=201",
                "offset=-1",
                "year=0",
                "municipality_id=1",
                "state_code=invalid",
                "start_date=not-a-date",
                "start_date=2025-02-01&end_date=2025-01-01",
            ]:
                response = self.client.get("/datasets/open_meteo_daily/records?" + query)
                self.assertEqual(response.status_code, 422, query)

            self.assertEqual(self.client.get("/datasets/geocoding/records?year=2025").status_code, 422)
            self.assertEqual(
                self.client.get("/datasets/open_meteo_daily/records?reference_date=2025-07-01").status_code,
                422,
            )

    def test_cors_is_limited_to_configured_frontend(self):
        response = self.client.options(
            "/datasets",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["access-control-allow-origin"], "http://localhost:5173")
        response = self.client.get("/health", headers={"Origin": "https://unconfigured.example"})
        self.assertNotIn("access-control-allow-origin", response.headers)
        self.assertEqual(self.client.post("/datasets").status_code, 405)

    def test_connection_uses_a_consistent_read_snapshot(self):
        connection = MagicMock()

        with patch("backend.core.database.psycopg.connect", return_value=connection):

            result = get_connection()

        self.assertIs(result, connection)
        self.assertEqual(connection.isolation_level, psycopg.IsolationLevel.REPEATABLE_READ)

    def test_bad_schema_configuration_is_rejected(self):

        with self.assertRaises(ValueError):

            Settings.load(gold_schema="gold; DROP TABLE municipalities")

    def test_connection_failure_does_not_expose_credentials(self):

        with patch(
            "backend.core.database.psycopg.connect", side_effect=psycopg.OperationalError("private-password")
        ):

            response = self.client.get("/overview")

        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private-password", response.text)



@unittest.skipUnless(
    os.environ.get("TEST_POSTGRES_DSN"), "Requer PostgreSQL temporário exclusivo para testes."
)
class GoldBackendIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.schema = "test_front_" + uuid4().hex[:12]
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.addCleanup(self.drop_schema)

        with psycopg.connect(os.environ["TEST_POSTGRES_DSN"]) as connection:

            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))

            for table in ["municipalities", "geocoding"]:
                connection.execute(
                    sql.SQL(
                        "CREATE TABLE {} (municipality_id bigint PRIMARY KEY, name text, state_code text, latitude double precision, longitude double precision)"
                    ).format(sql.Identifier(self.schema, table))
                )
                connection.execute(
                    sql.SQL(
                        "INSERT INTO {} VALUES (3550308,'São Paulo','SP',-23.55,-46.63),(3304557,'Rio de Janeiro','RJ',-22.90,-43.20)"
                    ).format(sql.Identifier(self.schema, table))
                )

            connection.execute(
                sql.SQL(
                    "CREATE TABLE {} (municipality_id bigint, reference_date date, state_code text, population_estimate bigint, PRIMARY KEY(municipality_id, reference_date))"
                ).format(sql.Identifier(self.schema, "population"))
            )
            connection.execute(
                sql.SQL("INSERT INTO {} VALUES (3550308,'2025-07-01','SP',10000)").format(
                    sql.Identifier(self.schema, "population")
                )
            )

            for table in ["inmet_daily", "open_meteo_daily"]:
                connection.execute(
                    sql.SQL(
                        "CREATE TABLE {} (municipality_id bigint, observation_date date, state_code text, temperature_mean_c double precision, wind_speed_max_ms double precision, quality_flags jsonb, PRIMARY KEY(municipality_id, observation_date))"
                    ).format(sql.Identifier(self.schema, table))
                )
                connection.execute(
                    sql.SQL(
                        "INSERT INTO {} VALUES (3550308,'2025-01-01','SP',20,3,'[]'),"
                        "(3550308,'2025-01-02','SP',NULL,4,'[\"invalid_numeric:temperature_mean_c\"]'),"
                        "(3304557,'2025-01-01','RJ',25,2,'[]'),(3550308,'2024-12-31','SP',19,3,'[]')"
                    ).format(sql.Identifier(self.schema, table))
                )

        def connection():
            return psycopg.connect(
                os.environ["TEST_POSTGRES_DSN"],
                options="-c default_transaction_read_only=on -c statement_timeout=5000",
            )

        repository = GoldRepository(self.schema, connection_factory=connection)
        self.client = TestClient(create_app(Path(self.temporary.name) / "audit", gold_repository=repository))
        self.addCleanup(self.client.close)

    def drop_schema(self):

        with psycopg.connect(os.environ["TEST_POSTGRES_DSN"]) as connection:

            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.schema))
            )

    def test_all_datasets_and_overview_can_be_read(self):
        response = self.client.get("/overview")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(response.json()["datasets"]), 5)
        self.assertIn("latest_load", response.json())

        for name in ["municipalities", "population", "geocoding", "inmet_daily", "open_meteo_daily"]:
            response = self.client.get("/datasets/" + name + "/records")
            self.assertEqual(response.status_code, 200, response.text)
            self.assertTrue(response.json()["items"])

    def test_population_prefers_latest_reference_and_keeps_date_filters(self):
        with psycopg.connect(os.environ["TEST_POSTGRES_DSN"]) as connection:

            connection.execute(
                sql.SQL("INSERT INTO {} VALUES (3550308,'2026-07-01','SP',12000)").format(
                    sql.Identifier(self.schema, "population")
                )
            )

        parameters = {"municipality_id": 3550308, "limit": 1}
        response = self.client.get("/datasets/population/records", params=parameters)
        self.assertEqual(response.status_code, 200)
        first = response.json()
        self.assertEqual(first["items"][0]["reference_date"], "2026-07-01")
        self.assertEqual(first["items"][0]["population_estimate"], 12000)
        self.assertTrue(first["has_more"])

        previous = self.client.get("/datasets/population/records", params={**parameters, "offset": 1}).json()
        self.assertEqual(previous["items"][0]["reference_date"], "2025-07-01")
        self.assertFalse(previous["has_more"])

        selected = self.client.get("/datasets/population/records", params={**parameters, "reference_date": "2025-07-01"}).json()
        self.assertEqual(selected["items"][0]["population_estimate"], 10000)
        self.assertFalse(selected["has_more"])

    def test_filters_and_page_boundaries(self):
        params = {"municipality_id": 3550308, "state_code": "SP", "year": 2025, "limit": 1}
        first = self.client.get("/datasets/open_meteo_daily/records", params=params).json()
        second = self.client.get("/datasets/open_meteo_daily/records", params={**params, "offset": 1}).json()
        self.assertTrue(first["has_more"])
        self.assertFalse(second["has_more"])
        self.assertEqual(first["items"][0]["observation_date"], "2025-01-01")
        self.assertEqual(second["items"][0]["observation_date"], "2025-01-02")
        self.assertIsNone(second["items"][0]["temperature_mean_c"])
        self.assertEqual(second["items"][0]["quality_flags"], ["invalid_numeric:temperature_mean_c"])
        response = self.client.get("/datasets/inmet_daily/records?start_date=2025-01-02&end_date=2025-01-02")
        self.assertEqual(len(response.json()["items"]), 1)

    def test_batch_keys_and_numeric_filters_preserve_exact_values(self):
        repository = self.client.app.state.gold_repository
        rows = repository.by_keys("municipalities", [{"municipality_id": 3550308}])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "São Paulo")
        self.assertFalse(repository.records("population", search_field="population_estimate", search_value="10000.1")["items"])
        self.assertEqual(len(repository.records("population", search_field="population_estimate", search_value="10000")["items"]), 1)

    def test_metadata_returns_types_units_and_reference_years(self):
        response = self.client.get("/datasets/open_meteo_daily")
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["years"], [2024, 2025])
        columns = {}

        for column in response.json()["columns"]:
            columns[column["name"]] = column

        self.assertEqual(columns["wind_speed_max_ms"]["unit"], "m/s")
        self.assertTrue(columns["municipality_id"]["primary_key"])
        response = self.client.get("/datasets/population/records?reference_date=2025-07-01")
        self.assertEqual(response.json()["items"][0]["population_estimate"], 10000)

    def test_missing_publication_is_explicit(self):

        with psycopg.connect(os.environ["TEST_POSTGRES_DSN"]) as connection:

            connection.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(self.schema, "population")))

        response = self.client.get("/datasets/population/records")
        self.assertEqual(response.status_code, 503)
        items = self.client.get("/overview").json()["datasets"]

        for dataset in items:

            if dataset["name"] == "population":
                self.assertFalse(dataset["available"])

    def test_repository_connection_is_read_only(self):
        factory = self.client.app.state.gold_repository.connection_factory

        with factory() as connection:

            self.assertEqual(connection.execute("SHOW transaction_read_only").fetchone()[0], "on")


if __name__ == "__main__":
    unittest.main()
