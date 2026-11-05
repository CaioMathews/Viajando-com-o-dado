"""Simula apenas o mecanismo de publicação; testes Spark conferem Parquet real separadamente."""

import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.shared import storage


class Schema:
    def simpleString(self):
        return "struct<id:bigint>"


class Writer:
    def mode(self, *args):
        return self

    def option(self, *args):
        return self

    def parquet(self, path):
        directory = Path(path)
        directory.mkdir()
        (directory / "part-00000.parquet").write_text("new-data", encoding="utf-8")


class Reader:
    def option(self, *args):
        return self

    def parquet(self, *args):
        return Frame()


class Session:
    read = Reader()


class Frame:
    schema = Schema()
    write = Writer()
    sparkSession = Session()

    def count(self):
        return 1


def main():
    output = Path(sys.argv[1]).resolve()
    phase = sys.argv[2]

    if phase == "hold-lock":
        with storage.output_lock(output):

            print("LOCKED " + str(os.getpid()), flush=True)
            sys.stdin.readline()

        return

    original_rename = storage.rename_output
    original_save = storage.save_transaction

    def renamed(source, destination):
        original_rename(source, destination)

        if phase == "backed_up" and source == output and ".backup-" in destination.name:
            os._exit(23)

        if phase == "published" and ".staging-" in source.name and destination == output:
            os._exit(23)

    def saved(journal, operation, state):
        original_save(journal, operation, state)

        if phase == state and state in ["writing", "prepared"]:
            os._exit(23)

    storage.rename_output = renamed
    storage.save_transaction = saved
    storage.write_parquet(Frame(), output, "test", False)


if __name__ == "__main__":
    main()
