import os
import json
from contextlib import contextmanager
from pathlib import Path
import shutil
import stat
from time import sleep
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.shared import execution


# Cada sistema utiliza sua biblioteca nativa de travamento.
if os.name == "nt":
    import msvcrt
else:
    import fcntl


def rename_output(
    source: Path,
    destination: Path,
) -> None:
    """Renomeia dentro da mesma pasta, aguardando bloqueios temporários do Windows."""

    source = source.resolve()
    destination = destination.resolve()

    if source.parent != destination.parent:
        raise ValueError("A renomeação deve permanecer na mesma pasta.")

    for attempt in range(6):

        try:

            source.rename(destination)

            return
        except PermissionError:

            if attempt == 5:
                raise

            sleep(0.5 * (attempt + 1))


def remove_work_directory(
    path: Path,
    parent: Path,
    name: str,
) -> None:
    """Remove somente a pasta auxiliar exata desta operação, nunca o destino final."""

    if path.is_symlink() or path.resolve().parent != parent or path.name != name:
        raise ValueError("Pasta auxiliar fora do destino autorizado.")

    def clear_readonly(function, failed_path, exception):
        failed = Path(failed_path).resolve()

        if failed != path.resolve() and path.resolve() not in failed.parents:
            raise ValueError("A limpeza tentou sair da pasta auxiliar autorizada.")

        if not isinstance(exception, PermissionError):
            raise exception

        # OneDrive pode manter o atributo somente leitura em arquivos e pastas movidos.
        os.chmod(failed_path, stat.S_IREAD | stat.S_IWRITE)
        function(failed_path)

    for attempt in range(6):

        try:

            if path.exists():
                shutil.rmtree(path, onexc=clear_readonly)

            return
        except PermissionError:

            if attempt == 5:
                raise

            sleep(0.5 * (attempt + 1))


# Exclusão entre escritores e recuperação de publicações interrompidas.


@contextmanager
def output_lock(output_directory: Path):
    """A trava pertence ao processo: o sistema operacional a libera inclusive em encerramento brusco."""

    lock = output_directory.parent / f".{output_directory.name}.write-lock"
    handle = lock.open("a+b")

    if lock.stat().st_size == 0:
        handle.write(b"0")
        handle.flush()

    handle.seek(0)

    try:

        if os.name == "nt":

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    except OSError as exception:
        handle.close()

        raise RuntimeError("Outra gravação está usando este destino.") from exception

    try:

        yield
    finally:
        handle.seek(0)

        if os.name == "nt":
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

        handle.close()

        # Mantém um único arquivo pequeno para impedir corridas entre processos na remoção.


def save_transaction(
    journal: Path,
    operation: str,
    phase: str,
) -> None:
    """Publica o registro de recuperação antes de mover qualquer carga válida."""

    temporary = journal.with_suffix(".tmp")

    with temporary.open("w", encoding="utf-8") as handle:

        json.dump({"operation": operation, "phase": phase}, handle)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, journal)


def recover_transaction(
    output_directory: Path,
) -> None:
    """Recupera somente a operação registrada; deve ser chamada com a trava adquirida."""

    parent = output_directory.parent
    journal = parent / f".{output_directory.name}.transaction.json"
    temporary = journal.with_suffix(".tmp")

    if not journal.exists():
        temporary.unlink(missing_ok=True)

        return

    transaction = json.loads(journal.read_text(encoding="utf-8"))
    operation = transaction.get("operation", "")
    phase = transaction.get("phase")

    valid_operation = len(operation) == 32

    for character in operation:

        if character not in "0123456789abcdef":
            valid_operation = False

    if not valid_operation or phase not in ["writing", "prepared", "published"]:
        raise ValueError(
            "Registro de recuperação inválido; os dados foram preservados para inspeção."
        )

    staging = parent / f".{output_directory.name}.staging-{operation}"
    backup = parent / f".{output_directory.name}.backup-{operation}"

    if not output_directory.exists() and backup.exists():
        rename_output(backup, output_directory)

    elif output_directory.exists() and backup.exists():

        if staging.exists():
            raise RuntimeError("Estado de recuperação ambíguo; nenhuma carga será descartada.")

        remove_work_directory(backup, parent, backup.name)

    elif not output_directory.exists() and phase in ["prepared", "published"]:

        if not staging.exists():
            raise RuntimeError("A operação registrada não possui carga recuperável.")

        rename_output(staging, output_directory)

    remove_work_directory(staging, parent, staging.name)
    journal.unlink()
    temporary.unlink(missing_ok=True)


def recover_output(
    output_directory: Path,
) -> None:
    """Permite recuperar uma carga interrompida antes de iniciar consultas ou novas cargas."""

    output_directory = output_directory.resolve()
    output_directory.parent.mkdir(parents=True, exist_ok=True)

    with output_lock(output_directory):

        recover_transaction(output_directory)


# Gravação temporária, validação e publicação da nova versão.


def write_parquet(
    dataframe: DataFrame,
    output_directory: Path,
    source_name: str,
    partition_by_state: bool = True,
    track_output: bool = True,
) -> None:
    """Valida a nova carga e recupera automaticamente operações interrompidas antes de escrever."""

    output_directory = output_directory.resolve()
    parent = output_directory.parent

    if not output_directory.name or not source_name or not source_name.replace("_", "").isalnum():
        raise ValueError("Destino ou nome da fonte inválido.")

    parent.mkdir(parents=True, exist_ok=True)

    with output_lock(output_directory):

        recover_transaction(output_directory)
        operation = uuid4().hex
        staging = parent / f".{output_directory.name}.staging-{operation}"
        backup = parent / f".{output_directory.name}.backup-{operation}"
        journal = parent / f".{output_directory.name}.transaction.json"
        save_transaction(journal, operation, "writing")

        try:

            export_data = dataframe

            if partition_by_state:
                invalid_state = dataframe.filter(
                    F.col("state_code").isNull() | ~F.col("state_code").rlike("^[A-Z]{2}$")
                )

                if invalid_state.limit(1).count() > 0:
                    raise ValueError("A carga contém UFs inválidas.")

                export_data = dataframe.repartition("state_code").withColumn(
                    "state_folder", F.col("state_code")
                )

            writer = export_data.write.mode("errorifexists").option(
                "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
            )

            if partition_by_state:
                writer.partitionBy("state_folder").parquet(staging.as_posix())
            else:
                writer.parquet(staging.as_posix())

            directories = []

            if partition_by_state:

                for partition in sorted(staging.glob("state_folder=*")):

                    state_code = partition.name.split("=", 1)[1]
                    state_directory = staging / state_code
                    rename_output(partition, state_directory)
                    directories.append((state_directory, state_code))

            else:
                directories.append((staging, ""))

            for directory, state_code in directories:

                parquet_files = sorted(directory.glob("part-*.parquet"))
                prefix = source_name

                if state_code:
                    prefix = f"{source_name}_{state_code}"

                for number, path in enumerate(parquet_files, start=1):

                    file_name = f"{prefix}.parquet"

                    if len(parquet_files) > 1:
                        file_name = f"{prefix}_{number:03d}.parquet"

                    checksum = path.with_name(f".{path.name}.crc")
                    rename_output(path, directory / file_name)

                    if checksum.exists():
                        rename_output(checksum, directory / f".{file_name}.crc")

            saved = (
                dataframe.sparkSession.read.option("recursiveFileLookup", True)
                .option("pathGlobFilter", "*.parquet")
                .parquet(staging.as_posix())
            )

            # Parquet pode tornar campos anuláveis; nomes e tipos devem permanecer iguais.
            saved_count = saved.count()

            if (
                saved.schema.simpleString() != dataframe.schema.simpleString()
                or saved_count != dataframe.count()
            ):
                raise ValueError(
                    "A nova carga não preservou o schema ou a quantidade de registros."
                )

            save_transaction(journal, operation, "prepared")

            if output_directory.exists():
                rename_output(output_directory, backup)

            rename_output(staging, output_directory)
            save_transaction(journal, operation, "published")
            recover_transaction(output_directory)

            if track_output:
                execution.set_output(output_directory, saved_count)
        except BaseException:
            # Recuperação também é repetida na próxima execução se o processo morrer aqui.
            recover_transaction(output_directory)

            raise
