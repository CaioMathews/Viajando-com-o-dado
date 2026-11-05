"""Publica coordenadas e clima juntos, com recuperação após falhas e interrupções."""

import json
import os
from pathlib import Path
import re
import shutil
from uuid import uuid4

from pipeline.shared import files
from pipeline.shared.storage import output_lock, remove_work_directory


# Caminhos e arquivos auxiliares da publicação por UF.

def collection_paths(data_directory: Path, state_code: str, year: str):
    """Deriva os destinos; o diário não pode indicar arquivos arbitrários."""

    if re.fullmatch(r"[A-Z]{2}", state_code) is None or re.fullmatch(r"[0-9]{4}", year) is None:
        raise ValueError("UF ou ano inválido para a publicação da coleta.")

    return [
        data_directory / "open_meteo/geocoding" / (state_code + ".json"),
        data_directory / "open_meteo/weather" / year / (state_code + ".json"),
    ]


def transaction_directory(data_directory: Path):
    root = data_directory.resolve() / ".collection_transactions"

    if root.is_symlink():
        raise ValueError("A pasta de recuperação da coleta não pode ser um link simbólico.")

    return root


def copy_durable(source: Path, destination: Path):
    shutil.copyfile(source, destination)

    with destination.open("r+b") as handle:

        handle.flush()
        os.fsync(handle.fileno())


def write_staged_json(path: Path, data):
    """Rejeita conteúdo não JSON antes de substituir qualquer destino válido."""

    with path.open("x", encoding="utf-8") as handle:

        json.dump(data, handle, ensure_ascii=False, indent=2, allow_nan=False)
        handle.flush()
        os.fsync(handle.fileno())


# Recuperação do par de JSONs antes de uma nova leitura ou publicação.

def recover_collection(data_directory: Path, state_code: str):
    """Recupera uma UF com sua trava adquirida; repetir a recuperação é seguro."""

    root = transaction_directory(data_directory)
    journal = root / (state_code + ".transaction.json")

    if not journal.exists():
        return

    transaction = json.loads(journal.read_text(encoding="utf-8"))
    operation = transaction.get("operation", "")
    phase = transaction.get("phase")
    year = transaction.get("year", "")

    if (
        transaction.get("state_code") != state_code
        or re.fullmatch(r"[0-9a-f]{32}", operation) is None
        or phase not in ["writing", "prepared", "published", "rolled_back"]
    ):
        raise ValueError("Diário da coleta inválido; os arquivos foram preservados para inspeção.")

    targets = collection_paths(data_directory, state_code, year)
    work = root / ("." + state_code + ".staging-" + operation)

    if work.is_symlink() or work.resolve().parent != root.resolve():
        raise ValueError("Pasta auxiliar da coleta fora do destino autorizado.")

    entries = transaction.get("files", [])

    if phase != "writing":

        if len(entries) != 2:
            raise ValueError("Diário da coleta sem as duas versões recuperáveis.")

        # Confere tudo antes de restaurar qualquer arquivo; não descarta versões desconhecidas.
        for index, target in enumerate(targets):
            entry = entries[index]
            before = entry.get("before_sha256")
            after = entry.get("after_sha256")

            if re.fullmatch(r"[0-9a-f]{64}", after or "") is None or (
                before is not None and re.fullmatch(r"[0-9a-f]{64}", before) is None
            ):
                raise ValueError("Hashes inválidos no diário da coleta.")

            current = files.digest_file(target) if target.is_file() else None
            expected = (
                [after] if phase == "published" else [before] if phase == "rolled_back" else [before, after]
            )

            if current not in expected:
                raise RuntimeError("Um destino da coleta mudou fora da operação; recuperação interrompida.")

            if phase == "prepared" and before is not None:
                backup = work / (str(index) + ".backup.json")

                if not backup.is_file() or files.digest_file(backup) != before:
                    raise ValueError("A cópia de recuperação da coleta está ausente ou alterada.")

        if phase == "prepared":

            for index, target in enumerate(targets):
                before = entries[index]["before_sha256"]

                if before is None:
                    target.unlink(missing_ok=True)
                else:
                    restored = work / (str(index) + ".restore.json")
                    copy_durable(work / (str(index) + ".backup.json"), restored)
                    files.replace_file(restored, target)

            transaction["phase"] = "rolled_back"
            files.save_json(journal, transaction)

    remove_work_directory(work, root.resolve(), work.name)
    journal.unlink()


def recover_collections(data_directory: Path):
    """Recupera publicações interrompidas antes de uma nova coleta ou leitura Bronze."""

    data_directory = data_directory.resolve()
    root = transaction_directory(data_directory)

    if not root.exists():
        return

    for journal in sorted(root.glob("*.transaction.json")):
        state_code = journal.name.removesuffix(".transaction.json")

        if re.fullmatch(r"[A-Z]{2}", state_code) is None:
            raise ValueError("Nome de diário da coleta inválido.")

        with output_lock(root / state_code):

            recover_collection(data_directory, state_code)


# Preparação, publicação e confirmação conjunta de coordenadas e clima.

def save_collection(coordinates, weather, data_directory: Path, state_code: str, year: str):
    """Prepara ambos os JSONs e só confirma o par depois de publicar os dois."""

    data_directory = data_directory.resolve()
    targets = collection_paths(data_directory, state_code, year)
    root = transaction_directory(data_directory)
    root.mkdir(parents=True, exist_ok=True)

    with output_lock(root / state_code):

        recover_collection(data_directory, state_code)
        operation = uuid4().hex
        work = root / ("." + state_code + ".staging-" + operation)
        journal = root / (state_code + ".transaction.json")
        transaction = {
            "state_code": state_code,
            "year": year,
            "operation": operation,
            "phase": "writing",
            "files": [],
        }
        files.save_json(journal, transaction)

        try:

            work.mkdir()

            for index, data in enumerate([coordinates, weather]):
                target = targets[index]
                staged = work / (str(index) + ".new.json")
                write_staged_json(staged, data)
                target.parent.mkdir(parents=True, exist_ok=True)
                before = None

                if target.exists():
                    before = files.digest_file(target)
                    backup = work / (str(index) + ".backup.json")
                    copy_durable(target, backup)

                    if files.digest_file(backup) != before:
                        raise RuntimeError("A coleta anterior mudou durante a preparação.")

                transaction["files"].append(
                    {"before_sha256": before, "after_sha256": files.digest_file(staged)}
                )

            transaction["phase"] = "prepared"
            files.save_json(journal, transaction)

            for index, target in enumerate(targets):
                files.replace_file(work / (str(index) + ".new.json"), target)

            transaction["phase"] = "published"
            files.save_json(journal, transaction)
            recover_collection(data_directory, state_code)
        except BaseException:
            recover_collection(data_directory, state_code)

            raise


def recover_inputs(parameters):
    """Recupera os JSONs antes de uma nova leitura."""

    directory = Path(parameters["input_directory"]).resolve()

    if directory.name == "geocoding" and directory.parent.name == "open_meteo":
        recover_collections(directory.parents[1])

    elif directory.parent.name == "weather" and directory.parent.parent.name == "open_meteo":
        recover_collections(directory.parents[2])
