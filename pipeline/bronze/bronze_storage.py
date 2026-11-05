import sys
from pathlib import Path

from pyspark.sql import DataFrame

# Compatibilidade com os imports locais do notebook da Bronze.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.shared.storage import write_parquet
from pipeline.shared.collection_storage import recover_inputs


def prepare_inputs(parameters):
    """Valida os caminhos antes de recuperar ou ler as fontes brutas."""

    input_directory = Path(parameters["input_directory"]).resolve()
    output_directory = Path(parameters["output_directory"]).resolve()

    if (
        input_directory == output_directory
        or input_directory in output_directory.parents
        or output_directory in input_directory.parents
    ):
        raise ValueError("As pastas de entrada e saída devem ser separadas, sem sobreposição.")

    recover_inputs(parameters)


def write_bronze(
    dataframe: DataFrame,
    output_directory: Path,
    source_name: str,
    partition_by_state: bool = True,
) -> None:
    """Grava a Bronze com validação prévia e recuperação da carga anterior em falhas."""

    write_parquet(dataframe, output_directory, source_name, partition_by_state)
