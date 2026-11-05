"""Fluxo atual, dados por nó, busca aproximada e vínculos entre registros."""

import json
from typing import Annotated

from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse

from pipeline.shared.catalog import catalog
from pipeline.shared import execution


router = APIRouter(prefix="/pipeline", tags=["Pipeline atual"])


@router.get("")
def graph(request: Request):
    state = execution.current(request.app.state.current_data.directory / "pipeline_status.json")
    reader = request.app.state.current_data
    reader.year = state.get("year", "2025")
    nodes = catalog(reader.year)
    edges = []

    for node in nodes:
        status_id = "load:postgres" if node["layer"] == "load" else node["id"]

        if node["layer"] == "source" and node["dataset"] in {"municipalities", "geocoding", "open_meteo_daily"}:
            status_id = "consume:consume"

        step = state["nodes"].get(status_id)
        node["execution"] = step

        try:

            if node["layer"] == "source":
                node["files_count"] = len(reader.source_files(node["id"]))
                node["available"] = node["files_count"] > 0
            else:
                node["available"] = bool(reader.paths(node))
        except FileNotFoundError:
            node["available"] = False

        node["status"] = step["status"] if step else "waiting" if state["status"] == "running" and node["layer"] != "source" else "available" if node["available"] else "not_started"
        node["records"] = step.get("records") if step else None

        if node["layer"] == "load" and step and step["status"] == "succeeded":
            node["records"] = request.app.state.gold_repository.metadata(node["dataset"])["records"]

        for parent in node["parents"]:
            edges.append({"source": parent, "target": node["id"], "kind": "aggregation" if node["id"] == "gold:inmet_daily" else "join" if len(node["parents"]) > 1 else "transform"})

    return {"execution": state, "nodes": nodes, "edges": edges}


@router.get("/suggestions")
def suggest(request: Request, q: Annotated[str, Query(max_length=200)] = ""):
    return {"items": request.app.state.discovery.suggest(q)}


@router.get("/nodes/{node_id}")
def metadata(node_id: str, request: Request):
    return request.app.state.current_data.metadata(node_id)


@router.get("/nodes/{node_id}/records")
def records(node_id: str, request: Request, q: Annotated[str, Query(max_length=500)] = "", limit: Annotated[int, Query(ge=1, le=100)] = 10, offset: Annotated[int, Query(ge=0)] = 0):
    reader = request.app.state.current_data
    parsed = request.app.state.discovery.parse(q) if q else {"filters": {}, "suggestions": [], "message": ""}

    if parsed["suggestions"]:
        return {**parsed, "items": [], "has_more": False, "next_offset": None}

    if "observation_date" in parsed["filters"] and reader.node(node_id)["dataset"] == "population":
        parsed["filters"]["reference_date"] = parsed["filters"].pop("observation_date")

    result = reader.records(node_id, parsed["filters"], limit=limit, offset=offset)

    return {**result, "suggestions": [], "message": parsed["message"]}


@router.get("/nodes/{node_id}/files")
def files(node_id: str, request: Request):
    return {"items": request.app.state.current_data.source_files(node_id)}


@router.get("/file")
def source_file(request: Request, path: Annotated[str, Query(max_length=1000)]):
    reader = request.app.state.current_data
    requested = (reader.directory / path).resolve()

    if not requested.is_relative_to(reader.directory) or not requested.is_file():
        raise FileNotFoundError()

    allowed = False

    for node in catalog(reader.year):
        if node["layer"] == "source" and requested.is_relative_to((reader.directory / node["path"]).resolve()):
            allowed = requested.suffix.lower() in {".json", ".csv", ".xls", ".xlsx"}

    if not allowed:
        raise ValueError("Arquivo fora das fontes cadastradas.")

    return FileResponse(requested, filename=requested.name, media_type="application/octet-stream")


@router.get("/search")
def search(request: Request, q: Annotated[str, Query(min_length=1, max_length=500)], limit: Annotated[int, Query(ge=1, le=50)] = 20, dataset_index: Annotated[int, Query(ge=0, le=5)] = 0, offset: Annotated[int, Query(ge=0, le=1000000)] = 0):
    reader = request.app.state.current_data
    reader.year = execution.current(reader.directory / "pipeline_status.json").get("year", "2025")

    with reader.session():

        return request.app.state.discovery.search(q, limit, dataset_index, offset)


@router.get("/journey")
def journey(request: Request, node_id: str, key: Annotated[str, Query(max_length=4000)]):
    parsed = json.loads(key)

    if not isinstance(parsed, dict) or not parsed:
        raise ValueError("Informe uma chave de registro válida.")

    for value in parsed.values():
        if not isinstance(value, (str, int, float)) or isinstance(value, bool):
            raise ValueError("Valores inválidos na chave.")

    with request.app.state.current_data.session():

        return request.app.state.journey_service.complete(node_id, parsed)
