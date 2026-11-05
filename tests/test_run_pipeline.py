import os
import json
import contextlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline import run_pipeline as runner
from pipeline.shared import execution


class PipelineOrchestrationTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "0"})
        environment.start()
        self.addCleanup(environment.stop)
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.events = []
        self.spark = MagicMock()
        self.preflight = self.stack.enter_context(patch.object(runner, 'validate_inputs'))
        self.create_spark = self.stack.enter_context(patch.object(runner, 'create_spark', return_value=self.spark))
        self.stack.enter_context(patch.object(runner.postgres, 'get_connection'))
        self.create_database = self.stack.enter_context(patch.object(runner.postgres, 'create_database'))
        self.actions = {}

        for name in ['consume', 'bronze', 'silver', 'gold']:
            self.actions[name] = self.stack.enter_context(patch.object(runner, 'run_' + name, side_effect=self.record(name)))

        self.load = self.stack.enter_context(patch.object(runner.postgres, 'load_gold', side_effect=self.record('load', {'municipalities': 27})))

    def record(self, name, result=None):
        def called(*arguments, **options):
            self.events.append(name)

            return result

        return called

    def test_default_runs_all_stages_in_order_and_closes_spark(self):
        summary = runner.run_pipeline()
        self.assertEqual(self.events, ['consume', 'bronze', 'silver', 'gold', 'load'])
        self.assertEqual(summary, {'municipalities': 27})
        self.create_spark.assert_called_once()
        self.spark.stop.assert_called_once()

    def test_gold_failure_prevents_database_load_and_closes_spark(self):
        self.actions['gold'].side_effect = ValueError('Falha simulada')

        with self.assertRaises(ValueError):

            runner.run_pipeline()

        self.load.assert_not_called()
        self.create_database.assert_not_called()
        self.spark.stop.assert_called_once()

    def test_shutdown_failure_preserves_processing_error(self):
        original = ValueError("Falha no processamento")
        self.actions["gold"].side_effect = original
        self.spark.stop.side_effect = ConnectionRefusedError("JVM indisponível")

        with self.assertLogs(runner.LOGGER, level="WARNING"):

            with self.assertRaises(ValueError) as caught:

                runner.run_pipeline(start_at="gold", stop_after="load")

        self.assertIs(caught.exception, original)
        self.load.assert_not_called()
        self.spark.stop.assert_called_once()

    def test_shutdown_failure_after_success_remains_a_failure(self):
        original = ConnectionRefusedError("JVM indisponível")
        self.spark.stop.side_effect = original

        with self.assertLogs(runner.LOGGER, level="WARNING"):

            with self.assertRaises(ConnectionRefusedError) as caught:

                runner.run_pipeline(start_at="gold", stop_after="gold")

        self.assertIs(caught.exception, original)
        self.assertEqual(self.events, ["gold"])

    def test_shutdown_failure_does_not_replace_processing_interrupt(self):
        original = KeyboardInterrupt()
        self.actions["gold"].side_effect = original
        self.spark.stop.side_effect = ConnectionRefusedError("JVM indisponível")

        with self.assertLogs(runner.LOGGER, level="WARNING"):

            with self.assertRaises(KeyboardInterrupt) as caught:

                runner.run_pipeline(start_at="gold", stop_after="gold")

        self.assertIs(caught.exception, original)

    def test_status_preserves_primary_error_and_redacts_shutdown_log(self):
        original = ValueError("Falha no processamento")
        self.actions["gold"].side_effect = original
        self.spark.stop.side_effect = ConnectionRefusedError("password=temporary-secret")

        with tempfile.TemporaryDirectory() as temporary:

            with (
                patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "1", "PGPASSWORD": "temporary-secret"}),
                self.assertLogs(runner.LOGGER, level="WARNING") as logs,
            ):

                with self.assertRaises(ValueError) as caught:

                    with patch.object(execution, "STATUS_PATH", Path(temporary) / "status.json"), execution.run_context() as run:

                        runner.run_pipeline(start_at="gold", stop_after="gold")

            self.assertIs(caught.exception, original)
            metadata = run
            self.assertEqual(metadata["status"], "failed")
            self.assertEqual(metadata["error"]["type"], "ValueError")
            self.assertEqual(metadata["error"]["message"], "Falha no processamento")
            self.assertNotIn("temporary-secret", "\n".join(logs.output))


    def test_failed_collection_does_not_start_spark_or_bronze(self):
        self.actions['consume'].side_effect = subprocess.CalledProcessError(1, 'consume.py')

        with self.assertRaises(subprocess.CalledProcessError):

            runner.run_pipeline()

        self.create_spark.assert_not_called()
        self.actions['bronze'].assert_not_called()

    def test_can_skip_collection_and_stop_before_database(self):
        runner.run_pipeline(start_at='bronze', stop_after='gold')
        self.assertEqual(self.events, ['bronze', 'silver', 'gold'])
        self.create_database.assert_not_called()
        self.load.assert_not_called()

    def test_load_can_run_independently(self):
        runner.run_pipeline(start_at='load', year='2024', schema_name='analytics')
        self.assertEqual(self.events, ['load'])
        self.load.assert_called_once_with(self.spark, ROOT / 'data/gold', year='2024', schema_name='analytics')

    def test_collection_only_does_not_start_spark(self):
        runner.run_pipeline(stop_after='consume')
        self.assertEqual(self.events, ['consume'])
        self.create_spark.assert_not_called()

    def test_preflight_failure_prevents_all_work(self):
        self.preflight.side_effect = FileNotFoundError('Borda ausente')

        with self.assertRaises(FileNotFoundError):

            runner.run_pipeline()

        self.assertEqual(self.events, [])
        self.create_spark.assert_not_called()

    def test_invalid_stage_order_is_rejected(self):
        with self.assertRaises(ValueError):

            runner.run_pipeline(start_at='gold', stop_after='bronze')

        self.preflight.assert_not_called()


class PipelineWiringTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "0"})
        environment.start()
        self.addCleanup(environment.stop)

    def test_missing_boundary_is_detected_before_collection(self):
        with tempfile.TemporaryDirectory() as temporary:

            root = Path(temporary)
            (root / 'data/ibge/population').mkdir(parents=True)
            (root / 'data/ibge/population/example.xls').touch()
            (root / 'data/inmet/2025').mkdir(parents=True)
            (root / 'data/inmet/2025/example.CSV').touch()
            jar = root / 'pipeline/bronze/lib/hadoop-bare-naked-local-fs-0.1.0.jar'
            jar.parent.mkdir(parents=True)
            jar.touch()

            with self.assertRaisesRegex(FileNotFoundError, 'boundary_2025'):

                runner.validate_inputs(root, '2025', runner.STAGES)

    def test_collector_subprocess_failure_is_propagated(self):
        with patch.object(runner.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'consume.py')) as process:

            with self.assertRaises(subprocess.CalledProcessError):

                runner.run_consume(ROOT, '2024')

            self.assertEqual(process.call_args.args[0], [sys.executable, str(ROOT / 'pipeline/consume APIs/consume.py'), '--year', '2024'])
            self.assertEqual(process.call_args.kwargs['cwd'], ROOT)
            self.assertTrue(process.call_args.kwargs['check'])

    def test_gold_uses_generated_catalog_and_explicit_boundary(self):
        with contextlib.ExitStack() as stack:

            actions = []

            for module, name in [(runner.gold_municipalities, 'build_municipalities'), (runner.gold_population, 'build_population'), (runner.gold_geocoding, 'build_geocoding'), (runner.gold_open_meteo, 'build_open_meteo'), (runner.gold_inmet, 'build_inmet')]:
                actions.append(stack.enter_context(patch.object(module, name)))

            spark = MagicMock()
            runner.run_gold(spark, ROOT / 'data', '2025')
            self.assertEqual(actions[1].call_args.args[2], ROOT / 'data/gold/ibge/municipalities')
            self.assertEqual(actions[4].call_args.kwargs['boundary_directory'], ROOT / 'data/silver/inmet/boundary_2025')
            self.assertEqual(actions[4].call_args.kwargs['minimum_station_coverage'], 0.9)


if __name__ == '__main__':
    unittest.main()
