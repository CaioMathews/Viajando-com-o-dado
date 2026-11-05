from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.shared.quality import has_rows


def validate_capitals(
    spark: SparkSession,
    capitals: DataFrame,
) -> None:
    """Confere os 27 pares oficiais de ID, nome e UF, além das coordenadas."""

    capital_records = [
        ("AC", 1200401, "Rio Branco"),
        ("AL", 2704302, "Maceió"),
        ("AM", 1302603, "Manaus"),
        ("AP", 1600303, "Macapá"),
        ("BA", 2927408, "Salvador"),
        ("CE", 2304400, "Fortaleza"),
        ("DF", 5300108, "Brasília"),
        ("ES", 3205309, "Vitória"),
        ("GO", 5208707, "Goiânia"),
        ("MA", 2111300, "São Luís"),
        ("MG", 3106200, "Belo Horizonte"),
        ("MS", 5002704, "Campo Grande"),
        ("MT", 5103403, "Cuiabá"),
        ("PA", 1501402, "Belém"),
        ("PB", 2507507, "João Pessoa"),
        ("PE", 2611606, "Recife"),
        ("PI", 2211001, "Teresina"),
        ("PR", 4106902, "Curitiba"),
        ("RJ", 3304557, "Rio de Janeiro"),
        ("RN", 2408102, "Natal"),
        ("RO", 1100205, "Porto Velho"),
        ("RR", 1400100, "Boa Vista"),
        ("RS", 4314902, "Porto Alegre"),
        ("SC", 4205407, "Florianópolis"),
        ("SE", 2800308, "Aracaju"),
        ("SP", 3550308, "São Paulo"),
        ("TO", 1721000, "Palmas"),
    ]

    capital_reference = spark.createDataFrame(
        capital_records,
        "expected_state_code string, municipality_id long, expected_name string",
    )

    if capitals.count() != 27 or capitals.select("municipality_id").distinct().count() != 27:
        raise ValueError("A referência deve conter exatamente as 27 capitais, sem repetições.")

    actual = capitals.select("municipality_id", "name", "state_code")
    comparison = capital_reference.join(actual, "municipality_id", "full")

    invalid = comparison.filter(
        ~F.col("name").eqNullSafe(F.col("expected_name"))
        | ~F.col("state_code").eqNullSafe(F.col("expected_state_code"))
    )

    if has_rows(invalid):
        raise ValueError("IDs, nomes ou UFs não correspondem às 27 capitais oficiais do IBGE.")

    latitude = "latitude" if "latitude" in capitals.columns else "capital_latitude"
    longitude = "longitude" if "longitude" in capitals.columns else "capital_longitude"

    bad_locations = capitals.filter(
        F.col(latitude).isNull()
        | F.isnan(latitude)
        | ~F.col(latitude).between(-90, 90)
        | F.col(longitude).isNull()
        | F.isnan(longitude)
        | ~F.col(longitude).between(-180, 180)
    )

    if has_rows(bad_locations):
        raise ValueError("Existem coordenadas inválidas na referência das capitais.")
