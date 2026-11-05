from pipeline.shared.http import get_json


def get_weather(latitude: float, longitude: float, start_date: str, end_date: str) -> dict:
    """Retorna o clima diário das coordenadas no período informado (AAAA-MM-DD)."""

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_mean,temperature_2m_min,temperature_2m_max,precipitation_sum,wind_speed_10m_max",
        "timezone": "auto",  # usa o fuso horário da localização consultada
        "models": "era5",
    }

    return get_json(url, params=params, timeout=30)
