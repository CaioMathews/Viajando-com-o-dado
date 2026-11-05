"""Contrato de paginação e parâmetros usados pelas rotas de dados atuais."""

import math
from typing import Annotated

from fastapi import Query
from pydantic import BaseModel, field_serializer


Limit = Annotated[int, Query(ge=1, le=200)]
Offset = Annotated[int, Query(ge=0)]
Municipality = Annotated[int | None, Query(ge=1000000, le=9999999)]
State = Annotated[str | None, Query(pattern=r"^[A-Z]{2}$")]
Year = Annotated[int | None, Query(ge=1, le=9999)]


def preserve_special_numbers(value):
    """Representa números não finitos no transporte JSON, preservando valores e ausências."""

    if isinstance(value, float) and not math.isfinite(value):

        if math.isnan(value):
            return "NaN"

        return "Infinity" if value > 0 else "-Infinity"

    if isinstance(value, dict):
        result = {}

        for key, item in value.items():
            result[key] = preserve_special_numbers(item)

        return result

    if isinstance(value, (list, tuple)):
        result = []

        for item in value:
            result.append(preserve_special_numbers(item))

        return result

    return value


class Page(BaseModel):
    items: list[dict]
    limit: int
    offset: int
    has_more: bool
    next_offset: int | None

    @field_serializer("items", when_used="json")
    def serialize_items(self, items):
        # Aplica a representação também às listas e estruturas aninhadas dos registros.
        return preserve_special_numbers(items)
