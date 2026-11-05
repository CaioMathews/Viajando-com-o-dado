"""Reinícios não reprocessam dados e bloqueios temporários têm tentativas limitadas."""

import errno
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pipeline.shared import files
from pipeline import run_pipeline as runner


class RestartTests(unittest.TestCase):
    def test_linux_permission_lock_retries_atomic_write(self):
        with tempfile.TemporaryDirectory() as directory:

            target = Path(directory) / "state.json"
            target.write_text('{"old": true}', encoding="utf-8")
            original = Path.replace
            calls = []

            def replace(source, destination):
                calls.append(source)

                if len(calls) < 3:
                    raise PermissionError(errno.EACCES, "Locked")

                return original(source, destination)

            with patch.object(Path, "replace", replace), patch.object(files, "sleep") as sleep:

                files.save_json(target, {"new": True})

            self.assertEqual(len(calls), 3)
            self.assertEqual(sleep.call_count, 2)
            self.assertEqual(json.loads(target.read_text()), {"new": True})

    def test_permanent_denial_preserves_previous_json(self):
        with tempfile.TemporaryDirectory() as directory:

            target = Path(directory) / "state.json"
            target.write_text('{"old": true}', encoding="utf-8")

            with patch.object(Path, "replace", side_effect=PermissionError(errno.EACCES, "Denied")) as replace, patch.object(files, "sleep"):

                with self.assertRaises(PermissionError):

                    files.save_json(target, {"new": True})

            self.assertEqual(replace.call_count, 8)
            self.assertEqual(json.loads(target.read_text()), {"old": True})
            self.assertEqual(len(list(Path(directory).glob("*.tmp"))), 0)

    def test_published_database_is_detected(self):
        connection = MagicMock()
        connection.execute.return_value.fetchone.side_effect = [("pipeline_trace.current",), ({"year": "2025", "schema_name": "gold"},)]

        with patch.object(runner.postgres, "get_connection") as connect:

            connect.return_value.__enter__.return_value = connection
            self.assertTrue(runner.publication_exists("2025", "gold"))

    def test_existing_publication_never_runs_pipeline(self):
        with patch("sys.argv", ["run_pipeline", "--if-needed"]), patch.object(runner, "publication_exists", return_value=True), patch.object(runner, "run_pipeline") as run:

            self.assertEqual(runner.main(), 0)
            run.assert_not_called()

    def test_first_initialization_runs_pipeline(self):
        with patch("sys.argv", ["run_pipeline", "--if-needed"]), patch.object(runner, "publication_exists", return_value=False), patch.object(runner.execution, "current", return_value={"status": "not_started"}), patch.object(runner, "run_pipeline") as run:

            self.assertEqual(runner.main(), 0)
            run.assert_called_once()

    def test_manual_command_runs_even_with_existing_publication(self):
        with patch("sys.argv", ["run_pipeline", "--start-at", "bronze"]), patch.object(runner, "publication_exists", return_value=True) as check, patch.object(runner, "run_pipeline") as run:

            self.assertEqual(runner.main(), 0)
            check.assert_not_called()
            run.assert_called_once_with("2025", "bronze", "load", "gold")

    def test_failed_initialization_is_not_retried_automatically(self):
        with patch("sys.argv", ["run_pipeline", "--if-needed"]), patch.object(runner, "publication_exists", return_value=False), patch.object(runner.execution, "current", return_value={"status": "failed"}), patch.object(runner, "run_pipeline") as run, patch.object(runner.LOGGER, "exception"):

            self.assertEqual(runner.main(), 1)
            run.assert_not_called()
