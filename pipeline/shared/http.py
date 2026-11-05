"""Consulta HTTP usada pelos consumidores das APIs."""

import requests


def get_json(url, params=None, timeout=30):
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()

    return response.json()
