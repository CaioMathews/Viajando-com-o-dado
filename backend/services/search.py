"""Resolve nomes aproximados e filtros explícitos, sem corrigir IDs ou datas silenciosamente."""

from datetime import datetime
from difflib import SequenceMatcher
import re
import unicodedata


def normalize(value):
    characters = []

    for character in unicodedata.normalize("NFKD", str(value).casefold().strip()):
        if not unicodedata.combining(character):
            characters.append(character)

    return " ".join("".join(characters).split())


def suggestions(query, capitals, limit=5):
    query = normalize(query)

    if not query or "=" in query or ";" in query:
        return []

    matches = []

    for capital in capitals:
        score = 0

        for field in ["name", "state_name", "state_code", "municipality_id"]:
            value = normalize(capital.get(field, ""))

            if not value:
                continue

            current = SequenceMatcher(None, query, value).ratio()

            if query == value:
                current = 1
            elif len(query) >= 2 and query in value:
                current = 0.94

            if field == "municipality_id" and query != value:
                current = 0

            score = max(score, current)

        if score >= 0.55:
            matches.append({"name": capital["name"], "state_code": capital["state_code"], "municipality_id": capital["municipality_id"], "score": round(score, 3)})

    matches.sort(key=lambda item: (-item["score"], item["name"]))

    return matches[:limit]


def resolve(query, capitals):
    filters = {}
    corrected = []
    parts = query.strip().split(";")

    for index, part in enumerate(parts):
        part = part.strip()

        if not part:
            continue

        field, separator, value = part.partition("=")

        if not separator:
            value = part
            field = "municipality_id" if value.isdecimal() else "date" if re.fullmatch(r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}", value) else "capital"

        field = normalize(field)
        value = value.strip()

        if not value:
            raise ValueError("Informe um valor para o filtro.")

        if field in {"uf", "state_code"}:
            state = value.upper()
            valid = False

            for municipality in capitals:
                if municipality.get("state_code") == state:
                    valid = True

            if not valid:
                raise ValueError("Informe uma UF válida com duas letras.")

            filters["state_code"] = state
        elif field in {"name", "capital", "municipality_name", "cidade"}:
            matches = suggestions(value, capitals)
            exact = []

            for candidate in matches:
                if candidate["score"] == 1:
                    exact.append(candidate)

            if len(exact) == 1:
                filters["municipality_id"] = exact[0]["municipality_id"]
            elif matches and matches[0]["score"] >= 0.7 and (len(matches) == 1 or matches[0]["score"] - matches[1]["score"] >= 0.1):
                filters["municipality_id"] = matches[0]["municipality_id"]
                corrected.append("Resultados para " + matches[0]["name"] + " · " + matches[0]["state_code"])
            elif matches:
                for candidate in matches:
                    selected = "municipality_id=" + str(candidate["municipality_id"])
                    candidate["query"] = ";".join(parts[:index] + [selected] + parts[index + 1:])

                return {"filters": {}, "suggestions": matches, "message": "Selecione a capital desejada."}
            elif field == "capital":
                filters["text"] = normalize(value)
            else:
                raise ValueError("Nenhuma capital correspondente. Experimente parte do nome ou a UF.")
        elif field in {"municipality_id", "id_ibge"}:
            if not re.fullmatch(r"\d{7}", value):
                raise ValueError("O código IBGE deve conter sete dígitos.")

            filters["municipality_id"] = int(value)
        elif field in {"date", "data", "observation_date", "reference_date"}:
            day = None

            for pattern in ["%Y-%m-%d", "%d/%m/%Y"]:
                try:

                    day = datetime.strptime(value, pattern).date().isoformat()
                    break
                except ValueError:
                    continue

            if not day:
                raise ValueError("Data inválida. Use AAAA-MM-DD ou DD/MM/AAAA.")

            filters["reference_date" if field == "reference_date" else "observation_date"] = day
        else:
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", field):
                raise ValueError("Nome de coluna inválido.")

            if filters.get("field"):
                raise ValueError("Use uma coluna de medição por consulta; combine-a com município e data quando necessário.")

            filters.update(field=field, value=normalize(value))

    return {"filters": filters, "suggestions": [], "message": "; ".join(corrected)}
