"""Falhas ao salvar o status não contaminam a próxima execução."""

import unittest
from unittest.mock import patch

from pipeline.shared import execution


class ExecutionContextTests(unittest.TestCase):
    def test_run_resets_after_initial_or_final_save_failure(self):
        for failed_save in (1, 2):

            with self.subTest(failed_save=failed_save):

                calls = 0
                failed_run = None

                def save(path, state):
                    nonlocal calls, failed_run
                    calls += 1
                    failed_run = state["run_id"]

                    if calls == failed_save:
                        raise PermissionError("Status bloqueado")

                with (
                    patch.dict("os.environ", {"VCD_TRACKING_ENABLED": "1"}),
                    patch.object(execution, "save_json", side_effect=save),
                ):

                    with self.assertRaises(PermissionError):

                        with execution.run_context():

                            pass

                self.assertIsNone(execution.active_run())

                with patch.object(execution, "save_json"):

                    with execution.run_context() as state:

                        self.assertNotEqual(state["run_id"], failed_run)

                self.assertIsNone(execution.active_run())

    def test_step_resets_after_initial_or_final_save_failure(self):
        for failed_save in (2, 3):

            with self.subTest(failed_save=failed_save):

                calls = 0
                executed = []

                def save(path, state):
                    nonlocal calls
                    calls += 1

                    if calls == failed_save:
                        raise PermissionError("Status bloqueado")

                @execution.step("silver", "geocoding")
                def transform():
                    executed.append(True)

                with (
                    patch.dict("os.environ", {"VCD_TRACKING_ENABLED": "1"}),
                    patch.object(execution, "save_json", side_effect=save),
                ):

                    with execution.run_context() as outer:

                        with self.assertRaises(PermissionError):

                            transform()

                        self.assertIs(execution.active_run(), outer)
                        self.assertIsNone(execution._STEP.get())

                self.assertEqual(bool(executed), failed_save == 3)
                self.assertIsNone(execution.active_run())

    def test_nested_runs_preserve_outer_context(self):
        with (
            patch.dict("os.environ", {"VCD_TRACKING_ENABLED": "1"}),
            patch.object(execution, "save_json") as save,
        ):

            with execution.run_context() as outer:

                with execution.run_context() as inner:

                    self.assertIs(inner, outer)

                self.assertIs(execution.active_run(), outer)

            self.assertEqual(save.call_count, 2)
            self.assertIsNone(execution.active_run())

    def test_function_error_cleans_both_contexts(self):
        @execution.step("silver", "geocoding")
        def transform():
            raise ValueError("Dado inválido")

        with (
            patch.dict("os.environ", {"VCD_TRACKING_ENABLED": "1"}),
            patch.object(execution, "save_json"),
        ):

            with self.assertRaisesRegex(ValueError, "Dado inválido"):

                transform()

        self.assertIsNone(execution.active_run())
        self.assertIsNone(execution._STEP.get())
