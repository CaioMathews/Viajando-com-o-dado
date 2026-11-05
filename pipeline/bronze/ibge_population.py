import json
from pathlib import Path

import xlrd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pipeline.shared import execution
from pipeline.bronze.bronze_storage import prepare_inputs, write_bronze


@execution.step("bronze", "ibge_population", before_inputs=prepare_inputs)
def ingest_population(
    spark: SparkSession,
    input_directory: Path,
    output_directory: Path,
) -> DataFrame:
    """Lê a planilha de população do IBGE e preserva suas linhas na Bronze em Parquet."""

    input_files = sorted(input_directory.resolve().glob("*.xls"))

    if len(input_files) != 1:
        raise ValueError("A pasta de população deve conter exatamente uma planilha .xls.")

    input_path = input_files[0]
    sheet_name = "Municípios"

    schema = StructType(
        [
            StructField("source_file", StringType(), True),
            StructField("source_sheet", StringType(), True),
            StructField("source_row_number", LongType(), True),
            StructField("row_values_json", StringType(), True),
        ]
    )

    # Lê o arquivo .xls diretamente, sem depender do pandas.
    workbook = xlrd.open_workbook(str(input_path), on_demand=True)
    records = []

    try:

        worksheet = workbook.sheet_by_name(sheet_name)

        # Guarda as células de cada linha em JSON, mantendo textos, números e vazios.
        # Títulos e notas são mantidos; as colunas de população serão extraídas na Silver.
        for row_index in range(worksheet.nrows):

            row_values = worksheet.row_values(row_index)

            records.append(
                {
                    "source_file": input_path.as_posix(),
                    "source_sheet": sheet_name,
                    "source_row_number": row_index + 1,
                    "row_values_json": json.dumps(row_values, ensure_ascii=False, allow_nan=False),
                }
            )

    finally:
        # Libera os recursos da planilha mesmo se ocorrer um erro na leitura.
        workbook.release_resources()

    population = spark.createDataFrame(records, schema=schema)

    # Substitui a saída anterior desta fonte pelos dados da nova execução.
    write_bronze(population, output_directory, "population", partition_by_state=False)

    return population
