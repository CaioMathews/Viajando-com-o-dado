from pathlib import Path
import subprocess
import os
import signal
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.shared import storage
from storage_probe import Frame


class StorageRecoveryTests(unittest.TestCase):
    def assert_clean(self, parent):
        self.assertFalse(list(parent.glob(".*staging-*")))
        self.assertFalse(list(parent.glob(".*backup-*")))
        self.assertFalse(list(parent.glob(".*transaction*")))

    def test_abrupt_exit_in_each_publication_phase(self):
        for phase in ["writing", "prepared", "backed_up", "published"]:

            with (
                self.subTest(phase=phase),
                tempfile.TemporaryDirectory(prefix="storage-regression-") as directory,
            ):

                parent = Path(directory)
                output = parent / "output"
                output.mkdir()
                (output / "test.parquet").write_text("old-data", encoding="utf-8")
                child = subprocess.run(
                    [sys.executable, str(ROOT / "tests/storage_probe.py"), str(output), phase],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(child.returncode, 23, child.stderr)
                storage.recover_output(output)
                expected = "new-data" if phase == "published" else "old-data"
                self.assertEqual((output / "test.parquet").read_text(encoding="utf-8"), expected)
                storage.recover_output(output)
                self.assert_clean(parent)
                storage.write_parquet(Frame(), output, "test", False)
                self.assertEqual((output / "test.parquet").read_text(encoding="utf-8"), "new-data")
                self.assert_clean(parent)

    def test_first_load_recovers_valid_prepared_data(self):
        with tempfile.TemporaryDirectory(prefix="storage-first-") as directory:

            output = Path(directory) / "output"
            child = subprocess.run(
                [sys.executable, str(ROOT / "tests/storage_probe.py"), str(output), "prepared"],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(child.returncode, 23, child.stderr)
            storage.recover_output(output)
            self.assertEqual((output / "test.parquet").read_text(encoding="utf-8"), "new-data")
            self.assert_clean(Path(directory))

    def test_active_writer_is_not_unlocked_by_another_process(self):
        with tempfile.TemporaryDirectory(prefix="storage-lock-") as directory:

            output = Path(directory) / "output"
            child = subprocess.Popen(
                [sys.executable, str(ROOT / "tests/storage_probe.py"), str(output), "hold-lock"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            try:

                message = child.stdout.readline().strip().split()
                self.assertEqual(message[0], "LOCKED")
                worker_pid = int(message[1])

                with self.assertRaises(RuntimeError):

                    storage.recover_output(output)

                # No Windows, o executável do venv pode ser apenas um redirecionador.
                os.kill(worker_pid, signal.SIGTERM)
                child.wait(timeout=10)
                storage.recover_output(output)
            finally:
                if child.poll() is None:
                    child.kill()

                child.communicate(timeout=10)

    def test_corrupt_journal_preserves_files(self):
        with tempfile.TemporaryDirectory(prefix="storage-journal-") as directory:

            parent = Path(directory)
            output = parent / "output"
            output.mkdir()
            (output / "test.parquet").write_text("old-data", encoding="utf-8")
            journal = parent / ".output.transaction.json"
            journal.write_text('{"operation":"../../outside","phase":"prepared"}', encoding="utf-8")

            with self.assertRaises(ValueError):

                storage.recover_output(output)

            self.assertEqual((output / "test.parquet").read_text(encoding="utf-8"), "old-data")


if __name__ == "__main__":
    unittest.main()
