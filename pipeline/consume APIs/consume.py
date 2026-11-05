import argparse
from datetime import date
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
import sys

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.shared import execution
from pipeline.shared.collection_storage import recover_collections, save_collection

from ibge import get_municipalities
from geocoding import get_coordinates
from open_meteo import get_weather


DATA_DIRECTORY = Path(__file__).resolve().parents[2] / "data"

# Códigos IBGE das 26 capitais estaduais e de Brasília.
CAPITAL_IDS = {
    "AC": 1200401, "AL": 2704302, "AM": 1302603,
    "AP": 1600303, "BA": 2927408, "CE": 2304400,
    "DF": 5300108, "ES": 3205309, "GO": 5208707,
    "MA": 2111300, "MG": 3106200, "MS": 5002704,
    "MT": 5103403, "PA": 1501402, "PB": 2507507,
    "PE": 2611606, "PI": 2211001, "PR": 4106902,
    "RJ": 3304557, "RN": 2408102, "RO": 1100205,
    "RR": 1400100, "RS": 4314902, "SC": 4205407,
    "SE": 2800308, "SP": 3550308, "TO": 1721000,
}


def save_json(data, output_path: Path) -> None:
    """Cria a pasta de destino e salva os dados em JSON."""

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Prepara o JSON ao lado do destino para preservar a versão anterior se houver falha.
    temporary_path = None

    try:

        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=output_path.parent,
                                prefix=f".{output_path.name}.", suffix=".tmp", delete=False) as output_file:

            temporary_path = Path(output_file.name)
            json.dump(data, output_file, ensure_ascii=False, indent=2)
            output_file.flush()
            os.fsync(output_file.fileno())

        temporary_path.replace(output_path)

    finally:

        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


@execution.step("consume", "consume")
def consume(year: str = "2025") -> list[str]:
    """Consulta as 27 capitais e retorna as UFs com falha nesta execução."""

    year = str(year)

    start_date = date.fromisoformat(f"{year}-01-01").isoformat()
    end_date = date.fromisoformat(f"{year}-12-31").isoformat()

    recover_collections(DATA_DIRECTORY)

    failed_states = []

    municipalities = get_municipalities()
    municipalities_by_state = {}

    for municipality in municipalities:

        state_code = municipality["regiao-imediata"]["regiao-intermediaria"]["UF"]["sigla"]

        # Mantém somente a capital de cada UF, identificada pelo código oficial.
        if municipality["id"] != CAPITAL_IDS.get(state_code):
            continue

        municipalities_by_state.setdefault(state_code, []).append(municipality)


    # Confere se todas as capitais foram encontradas antes de salvar os arquivos.
    missing_states = set(CAPITAL_IDS) - set(municipalities_by_state)

    if missing_states:
        raise ValueError(f"Capitais ausentes no IBGE para as UFs: {', '.join(sorted(missing_states))}")

    # Salva os registros das capitais do IBGE antes de iniciar as consultas de clima.
    for state_code, state_municipalities in municipalities_by_state.items():
        save_json(
            state_municipalities,
            DATA_DIRECTORY / "ibge" / "municipalities" / f"{state_code}.json",
        )

    for state_code, state_municipalities in municipalities_by_state.items():
        state_coordinates = []
        state_weather = []
        state_errors = []

        for municipality in state_municipalities:
            city_name = municipality["nome"]
            municipality_id = municipality["id"]
            search_name = f"{city_name}, {state_code}"

            print(f"Consultando {search_name} ({municipality_id})...")

            stage = "geocoding"

            try:

                latitude, longitude = get_coordinates(search_name)

                state_coordinates.append({
                    "municipality_id": municipality_id,
                    "name": city_name,
                    "latitude": latitude,
                    "longitude": longitude,
                })

                stage = "open_meteo"
                weather_data = get_weather(latitude, longitude, start_date, end_date)

                state_weather.append({
                    "municipality_id": municipality_id,
                    "name": city_name,
                    "start_date": start_date,
                    "end_date": end_date,
                    "weather": weather_data,
                })

            except (requests.RequestException, ValueError) as error:
                # Registra a falha sem interromper os demais municípios.
                state_errors.append({
                    "municipality_id": municipality_id,
                    "name": city_name,
                    "stage": stage,
                    "error": str(error),
                })
                print(f"Falha em {search_name}: {error}")

        error_path = DATA_DIRECTORY / "errors" / f"{state_code}.json"

        if state_errors:
            failed_states.append(state_code)
            # Preserva os dois arquivos da UF para não misturar coletas antigas e incompletas.
            save_json(state_errors, error_path)
        else:
            save_collection(state_coordinates, state_weather, DATA_DIRECTORY, state_code, year)
            # Remove a falha anterior somente depois de salvar a coleta bem-sucedida.
            error_path.unlink(missing_ok=True)

        print(f"{state_code}: {len(state_weather)} municípios com clima; {len(state_errors)} falhas.")

    return failed_states


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coleta dados das capitais no IBGE e na Open-Meteo.")
    parser.add_argument("--year", default="2025", help="Ano meteorológico. Padrão: 2025.")

    arguments = parser.parse_args()
    failed_states = consume(arguments.year)

    if failed_states:
        raise SystemExit("Coleta incompleta nas UFs: " + ", ".join(failed_states))
