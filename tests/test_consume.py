import os
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIRECTORY = ROOT / "pipeline" / "consume APIs"


class ConsumeRegressionTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "0"})
        environment.start()
        self.addCleanup(environment.stop)

        # Carrega os imports locais sem interferir nos módulos das outras camadas.
        with patch.dict(sys.modules), patch.object(sys, "path", [str(SOURCE_DIRECTORY)] + sys.path):

            for name in ["ibge", "geocoding", "open_meteo"]:

                sys.modules.pop(name, None)

            spec = importlib.util.spec_from_file_location(
                "consume_under_test", SOURCE_DIRECTORY / "consume.py"
            )
            self.consume = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.consume)

        temporary = tempfile.TemporaryDirectory(prefix="consume-regression-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.consume.DATA_DIRECTORY = self.directory
        self.consume.CAPITAL_IDS = {"SP": 3550308}
        self.municipality = {
            "id": 3550308,
            "nome": "São Paulo",
            "regiao-imediata": {"regiao-intermediaria": {"UF": {"sigla": "SP"}}},
        }
        self.consume.get_municipalities = lambda: [self.municipality]
        self.consume.get_coordinates = lambda name: (-23.55, -46.63)
        self.consume.get_weather = lambda *args: {"daily": {"time": ["2025-01-01"]}}
        self.coordinates = self.directory / "open_meteo/geocoding/SP.json"
        self.weather = self.directory / "open_meteo/weather/2025/SP.json"
        self.error = self.directory / "errors/SP.json"
        self.previous = '[{"previous_valid_data": true}]'

    def seed(self):
        for path in [self.coordinates, self.weather]:

            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self.previous, encoding="utf-8")

    def run_consume(self):
        with contextlib.redirect_stdout(io.StringIO()):

            self.consume.consume()

    def fail_request(self, *args):
        raise self.consume.requests.Timeout("Falha simulada")

    def test_geocoding_failure_preserves_both_previous_files(self):
        self.seed()
        self.consume.get_coordinates = self.fail_request
        self.run_consume()

        for path in [self.coordinates, self.weather]:

            self.assertEqual(path.read_text(encoding="utf-8"), self.previous)

        self.assertEqual(
            json.loads(self.error.read_text(encoding="utf-8"))[0]["stage"], "geocoding"
        )

    def test_weather_failure_preserves_both_previous_files(self):
        self.seed()
        self.consume.get_weather = self.fail_request
        self.run_consume()

        for path in [self.coordinates, self.weather]:

            self.assertEqual(path.read_text(encoding="utf-8"), self.previous)

        self.assertEqual(
            json.loads(self.error.read_text(encoding="utf-8"))[0]["stage"], "open_meteo"
        )

    def test_first_failed_collection_does_not_create_empty_data(self):
        self.consume.get_coordinates = self.fail_request
        self.run_consume()
        self.assertFalse(self.coordinates.exists())
        self.assertFalse(self.weather.exists())
        self.assertTrue(self.error.exists())

    def test_success_replaces_data_and_removes_previous_error(self):
        self.seed()
        self.consume.get_weather = self.fail_request
        self.run_consume()
        self.assertTrue(self.error.exists())
        self.consume.get_weather = lambda *args: {"daily": {"time": ["2025-01-01"]}}
        self.run_consume()
        self.assertFalse(self.error.exists())
        self.assertEqual(
            json.loads(self.coordinates.read_text(encoding="utf-8"))[0]["latitude"], -23.55
        )
        self.assertEqual(
            json.loads(self.weather.read_text(encoding="utf-8"))[0]["municipality_id"], 3550308
        )

    def test_first_success_does_not_create_error_file(self):
        self.run_consume()
        self.assertTrue(self.coordinates.exists())
        self.assertTrue(self.weather.exists())
        self.assertFalse(self.error.exists())

    def test_serialization_failure_preserves_previous_json(self):
        self.seed()

        with self.assertRaises(TypeError):

            self.consume.save_json([object()], self.coordinates)

        self.assertEqual(self.coordinates.read_text(encoding="utf-8"), self.previous)
        self.assertEqual(list(self.coordinates.parent.glob("*.tmp")), [])

    def test_failed_states_are_reported_to_the_orchestrator(self):
        self.consume.get_coordinates = self.fail_request

        with contextlib.redirect_stdout(io.StringIO()):

            failed_states = self.consume.consume()

        self.assertEqual(failed_states, ["SP"])

    def test_selected_year_controls_request_and_output_directory(self):
        with patch.object(
            self.consume, "get_weather", return_value={"daily": {}}
        ) as weather_request:

            with contextlib.redirect_stdout(io.StringIO()):

                self.assertEqual(self.consume.consume("2024"), [])

        weather_request.assert_called_once_with(-23.55, -46.63, "2024-01-01", "2024-12-31")
        self.assertTrue((self.directory / "open_meteo/weather/2024/SP.json").is_file())
        self.assertFalse(self.weather.exists())


if __name__ == "__main__":
    unittest.main()
