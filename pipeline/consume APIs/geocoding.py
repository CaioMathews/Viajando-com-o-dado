import unicodedata

from pipeline.shared.http import get_json


# O retorno da API identifica o estado pelo nome completo, no campo admin1.
STATE_NAMES = {
    "AC": "Acre", "AL": "Alagoas", "AM": "Amazonas",
    "AP": "Amapá", "BA": "Bahia", "CE": "Ceará",
    "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
    "MA": "Maranhão", "MG": "Minas Gerais", "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso", "PA": "Pará", "PB": "Paraíba",
    "PE": "Pernambuco", "PI": "Piauí", "PR": "Paraná",
    "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RO": "Rondônia",
    "RR": "Roraima", "RS": "Rio Grande do Sul", "SC": "Santa Catarina",
    "SE": "Sergipe", "SP": "São Paulo", "TO": "Tocantins",
}


def normalize_name(name: str) -> str:
    """Permite comparar nomes sem diferenças de acentos e maiúsculas."""

    normalized_name = unicodedata.normalize("NFKD", name.strip().casefold())
    characters = []

    for character in normalized_name:
        if not unicodedata.combining(character):
            characters.append(character)

    return "".join(characters)

def get_coordinates(city_name: str) -> tuple[float, float]:
    """Consulta uma cidade brasileira, com UF opcional, e retorna suas coordenadas."""

    name, separator, state = city_name.partition(",")
    name = name.strip()
    state = state.strip()

    if not name:
        raise ValueError("Informe o nome de uma cidade.")

    if separator and not state:
        raise ValueError("Informe o estado após a vírgula.")

    state_name = STATE_NAMES.get(state.upper(), state)
    search_name = f"{name}, {state_name}" if state_name else name
    url = "https://geocoding-api.open-meteo.com/v1/search"

    params = {
        "name": search_name,
        "countryCode": "BR",
        "count": 100,
        "language": "pt",
        "format": "json",
    }

    locations = get_json(url, params=params, timeout=30).get("results", [])

    # Descarta nomes apenas semelhantes, outros estados e locais que não são povoados.
    candidates = []

    for location in locations:
        if location.get("country_code") != "BR":
            continue

        if normalize_name(location.get("name", "")) != normalize_name(name):
            continue

        if not location.get("feature_code", "").startswith("PPL"):
            continue

        if state_name:

            if normalize_name(location.get("admin1", "")) != normalize_name(state_name):
                continue

        candidates.append(location)

    # Havendo uma capital com o nome exato, prefere a sede estadual ou federal.
    capitals = []

    for location in candidates:

        if location.get("feature_code") in {"PPLA", "PPLC"}:
            capitals.append(location)

    if capitals:


        candidates = capitals



    if not candidates:
        raise ValueError(f"Nenhuma cidade correspondente encontrada para: {city_name}.")

    if len(candidates) > 1:

        raise ValueError(
            f"Mais de uma cidade corresponde a: {city_name}, mesmo após os filtros. "
            "Confira o nome e o estado informados."
        )

    location = candidates[0]

    return location["latitude"], location["longitude"]
