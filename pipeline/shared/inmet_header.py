from pipeline.shared.quality import has_rows

"""Contrato dos nomes, da ordem e das unidades dos CSVs horários do INMET."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# O último campo vazio corresponde ao ponto e vírgula que encerra cada linha.
EXPECTED_COLUMNS = [
    "DATA",
    "HORA UTC",
    "PRECIPITACAO TOTAL, HORARIO (MM)",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (MB)",
    "PRESSAO ATMOSFERICA MAX.NA HORA ANT. (AUT) (MB)",
    "PRESSAO ATMOSFERICA MIN. NA HORA ANT. (AUT) (MB)",
    "RADIACAO GLOBAL (KJ/M²)",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)",
    "TEMPERATURA MAXIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA MINIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
    "VENTO, DIRECAO HORARIA (GR) (° (GR))",
    "VENTO, RAJADA MAXIMA (M/S)",
    "VENTO, VELOCIDADE HORARIA (M/S)",
    "",
]


def validate_headers(dataframe: DataFrame) -> None:
    """Rejeita cabeçalhos incompatíveis antes de interpretar os valores por posição."""

    for column_name in ["source_file", "source_header"]:

        if column_name not in dataframe.columns:
            raise ValueError(
                "Cabeçalho INMET ausente na entrada; revise a fonte e gere novamente a Bronze."
            )

    headers = dataframe.select("source_file", "source_header").distinct()

    # Padroniza apenas apresentação; nomes, posições, pontuação e unidades continuam exigidos.
    normalized = F.upper(F.col("source_header"))
    normalized = F.translate(normalized, "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ", "AAAAAEEEEIIIIOOOOOUUUUC")
    normalized = F.trim(F.regexp_replace(normalized, r"\s+", " "))
    normalized = F.regexp_replace(normalized, r"\s*;\s*", ";")

    expected = ";".join(EXPECTED_COLUMNS)

    headers = headers.withColumn("normalized_header", normalized)

    invalid = headers.filter(~F.col("normalized_header").eqNullSafe(F.lit(expected)))
    invalid = invalid.withColumn("expected_header", F.lit(expected))

    if has_rows(invalid):
        raise ValueError(
            (
                "Cabeçalho INMET incompatível: confira os nomes, a ordem e as unidades das colunas antes de "
                "gerar a Bronze e a Silver."
            )
        )
