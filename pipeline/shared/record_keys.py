"""Identifica registros pelas chaves atuais, sem copiar seus valores nem criar versões."""

from datetime import date, datetime, timezone
import hashlib
import json
import math


def clean(value):
    if isinstance(value, datetime):

        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, float) and not math.isfinite(value):
        return None

    if isinstance(value, dict):

        result = {}

        for key, item in value.items():

            result[key] = clean(item)

        return result

    if isinstance(value, (list, tuple)):
        result = []

        for item in value:

            result.append(clean(item))

        return result

    return value


def record_key(node, row):
    key = {}

    for field in node["key_fields"]:

        key[field] = clean(row[field])

    return key


def record_id(node_id, key):
    value = json.dumps(clean(key), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    return hashlib.sha256((node_id + ":" + value).encode("utf-8")).hexdigest()
