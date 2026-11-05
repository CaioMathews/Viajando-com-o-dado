"""Gravações pequenas e duráveis usadas pelo controle da execução e pela recuperação."""

import errno
import hashlib
import json
import os
from pathlib import Path
from time import sleep
from uuid import uuid4


def digest_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:

        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def save_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")

    try:

        with temporary.open("w", encoding="utf-8") as handle:

            json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False, default=str)
            handle.flush()
            os.fsync(handle.fileno())

        replace_file(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def replace_file(source: Path, destination: Path) -> None:
    """Repete a troca atômica diante de bloqueios temporários do filesystem."""

    for attempt in range(8):
        try:

            source.replace(destination)

            return
        except PermissionError as error:
            # O Docker sobre Windows pode devolver EACCES sem o atributo winerror.
            temporary_lock = error.errno in {errno.EACCES, errno.EPERM} or getattr(error, "winerror", None) in {5, 32, 33}

            if not temporary_lock or attempt == 7:
                raise

            sleep(0.05 * (2 ** attempt))


def fingerprint(directory, suffixes=None):
    """Confere nomes, tamanhos e datas sem repetir stat nos mesmos arquivos."""

    directory = Path(directory)
    files = []
    selected = suffixes or {".parquet"}

    if not directory.is_dir():
        return files

    pending = [directory]

    while pending:
        current = pending.pop()

        with os.scandir(current) as entries:

            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif Path(entry.name).suffix.lower() in selected and entry.is_file():
                    # DirEntry reutiliza os metadados retornados pelo filesystem.
                    # Marcadores, CRCs e arquivos de outros tipos não exigem stat.
                    metadata = entry.stat()
                    relative = Path(entry.path).relative_to(directory).as_posix()
                    files.append([relative, metadata.st_size, metadata.st_mtime_ns])

    files.sort(key=lambda item: Path(item[0]))

    return files
