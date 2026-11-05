"""Dados e metadados publicados para tabelas, mapas, gráficos e filtros do front."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Request

from backend.repositories.gold import GoldRepository
from backend.schemas.common import Limit, Municipality, Offset, Page, State, Year


router = APIRouter(tags=["Dados publicados"])


def repository(request: Request) -> GoldRepository:
    return request.app.state.gold_repository


Gold = Annotated[GoldRepository, Depends(repository)]


@router.get("/datasets")
def datasets(store: Gold):
    return store.catalog()


@router.get("/overview")
def overview(request: Request, store: Gold):
    summary = store.overview()
    current = request.app.state.trace_store.state()

    return {**summary, "latest_load": current}



@router.get("/datasets/{dataset}")
def metadata(dataset: str, store: Gold):
    return store.metadata(dataset)


@router.get("/datasets/{dataset}/records", response_model=Page)
def records(
    dataset: str,
    store: Gold,
    municipality_id: Municipality = None,
    state_code: State = None,
    year: Year = None,
    start_date: date | None = None,
    end_date: date | None = None,
    reference_date: date | None = None,
    limit: Limit = 50,
    offset: Offset = 0,
):
    return store.records(
        dataset,
        municipality_id=municipality_id,
        state_code=state_code,
        year=year,
        start_date=start_date,
        end_date=end_date,
        reference_date=reference_date,
        limit=limit,
        offset=offset,
    )
