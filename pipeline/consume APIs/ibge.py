from pipeline.shared.http import get_json


def get_municipalities() -> list[dict]:
    """Consulta a API do IBGE e retorna a lista de municípios brasileiros."""

    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"

    return get_json(url, timeout=30)
