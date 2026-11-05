"""Confere a consistência do par coordenadas/clima, inclusive em encerramentos bruscos."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.shared import execution, files, collection_storage


@execution.step("bronze", "geocoding", before_inputs=collection_storage.recover_inputs)
def inspect_input(input_directory: Path):
    """Representa uma leitura para conferir que a recuperação precede a leitura."""

    return json.loads(next(input_directory.glob("*.json")).read_text(encoding="utf-8"))


class CollectionPublicationTests(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "0"})
        environment.start()
        self.addCleanup(environment.stop)
        temporary = tempfile.TemporaryDirectory(prefix="collection-pair-test-")
        self.addCleanup(temporary.cleanup)
        self.data = Path(temporary.name) / "data"
        status = patch.object(execution, "STATUS_PATH", self.data.parent / "status.json")
        status.start()
        self.addCleanup(status.stop)
        self.targets = collection_storage.collection_paths(self.data, "SP", "2025")
        self.old = [{"version": "old"}]
        self.new = [{"version": "new"}]

    def seed(self):
        for target in self.targets:

            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(self.old), encoding="utf-8")

    def assert_pair(self, expected):
        for target in self.targets:

            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), expected)

        root = collection_storage.transaction_directory(self.data)
        self.assertEqual(list(root.glob("*.transaction.json")), [])
        self.assertEqual(list(root.glob("*.staging-*")), [])

    def probe(self, phase):
        # os._exit simula a morte do processo, sem executar except/finally.
        program = """
import os
from pathlib import Path
import sys
from pipeline.shared import execution, files, collection_storage as storage
data = Path(sys.argv[1])
phase = sys.argv[2]
original_replace = os.replace
original_save = files.save_json
def replace(source, destination):
    source = Path(source)
    destination = Path(destination)
    if phase == "first_published" and source.name == "1.new.json":
        os._exit(23)
    original_replace(source, destination)
    if phase == "restore_interrupted" and source.name == "0.restore.json":
        os._exit(23)
def save(path, value):
    original_save(path, value)
    if phase == "committed" and isinstance(value, dict) and value.get("phase") == "published":
        os._exit(23)
os.replace = replace
files.save_json = save
if phase == "restore_interrupted":
    storage.recover_collections(data)
else:
    storage.save_collection([{"version": "new"}], [{"version": "new"}], data, "SP", "2025")
"""
        result = subprocess.run(
            [sys.executable, "-B", "-c", program, str(self.data), phase],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=45,
        )
        self.assertEqual(result.returncode, 23, result.stdout + result.stderr)

    def test_success_replaces_both_files(self):
        self.seed()
        collection_storage.save_collection(self.new, self.new, self.data, "SP", "2025")
        self.assert_pair(self.new)

    def test_second_publication_failure_restores_both_files(self):
        self.seed()
        original = os.replace

        def fail_second(source, destination):
            if Path(source).name == "1.new.json":
                raise OSError("Falha simulada ao publicar o clima")

            return original(source, destination)

        with patch.object(collection_storage.os, "replace", side_effect=fail_second):

            with self.assertRaises(OSError):

                collection_storage.save_collection(self.new, self.new, self.data, "SP", "2025")

        self.assert_pair(self.old)

    def test_serialization_failure_preserves_previous_pair(self):
        self.seed()

        with self.assertRaises(TypeError):

            collection_storage.save_collection(self.new, [object()], self.data, "SP", "2025")

        self.assert_pair(self.old)

    def test_preparation_journal_failure_preserves_previous_pair(self):
        self.seed()
        original = files.save_json

        def fail_preparation(path, value):
            if value.get("phase") == "prepared":
                raise ValueError("Falha simulada ao confirmar a preparação")

            return original(path, value)

        with patch.object(files, "save_json", side_effect=fail_preparation):

            with self.assertRaises(ValueError):

                collection_storage.save_collection(self.new, self.new, self.data, "SP", "2025")

        self.assert_pair(self.old)

    def test_crash_during_first_load_does_not_leave_partial_data(self):
        self.probe("first_published")
        collection_storage.recover_collections(self.data)

        for target in self.targets:

            self.assertFalse(target.exists())

    def test_crash_and_interrupted_recovery_restore_previous_pair(self):
        self.seed()
        self.probe("first_published")
        self.probe("restore_interrupted")
        collection_storage.recover_collections(self.data)
        collection_storage.recover_collections(self.data)
        self.assert_pair(self.old)

    def test_committed_pair_survives_crash_before_cleanup(self):
        self.seed()
        self.probe("committed")
        collection_storage.recover_collections(self.data)
        self.assert_pair(self.new)

    def test_corrupted_backup_blocks_recovery(self):
        self.seed()
        self.probe("first_published")
        root = collection_storage.transaction_directory(self.data)
        backup = next(root.glob("*.staging-*/0.backup.json"))
        backup.write_text("corrupted", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "recuperação"):

            collection_storage.recover_collections(self.data)

        self.assertTrue((root / "SP.transaction.json").exists())

    def test_bronze_reads_recovered_pair(self):
        self.seed()
        self.probe("first_published")

        with patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "1"}):

            with execution.run_context(ROOT):

                result = inspect_input(self.targets[0].parent)

        self.assertEqual(result, self.old)
        self.assert_pair(self.old)
        self.assertEqual(execution.current()["status"], "succeeded")
        self.assertFalse((self.data / "audit").exists())

    def test_notebook_recovery_failure_is_not_recorded_as_success(self):
        self.seed()
        self.probe("first_published")
        root = collection_storage.transaction_directory(self.data)
        next(root.glob("*.staging-*/0.backup.json")).write_text("corrupted", encoding="utf-8")

        with patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "1"}):

            session = execution.run_context(ROOT)
            session.__enter__()

            try:

                with self.assertRaisesRegex(ValueError, "recuperação"):

                    inspect_input(self.targets[0].parent)
            finally:
                session.__exit__(None, None, None)

        state = execution.current()
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["nodes"]["bronze:geocoding"]["status"], "failed")
        self.assertEqual(state["nodes"]["bronze:geocoding"]["error"]["type"], "ValueError")

    def test_standalone_recovery_failure_is_reported(self):
        self.seed()
        self.probe("first_published")
        root = collection_storage.transaction_directory(self.data)
        next(root.glob("*.staging-*/0.backup.json")).write_text("corrupted", encoding="utf-8")

        with patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "1"}):

            with self.assertRaisesRegex(ValueError, "recuperação"):

                inspect_input(self.targets[0].parent)

        self.assertEqual(execution.current()["status"], "failed")
        self.assertEqual(execution.current()["error"]["type"], "ValueError")


if __name__ == "__main__":
    unittest.main()
