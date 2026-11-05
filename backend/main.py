"""Monta a API de dados e rastreamento atual sem iniciar Spark ou conectar ao banco na importação."""

import json
import logging
from pathlib import Path

import duckdb
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg

from backend.core.config import Settings
from backend.repositories.gold import DatasetNotFoundError, DatasetUnavailableError, GoldRepository
from backend.routes.pipeline import router as pipeline_router
from backend.repositories.current_data import CurrentData
from backend.services.trace import TraceStore, TraceService, TraceUnavailableError
from backend.services.journey import JourneyService
from backend.services.discovery import DiscoveryService
from backend.routes.datasets import router as datasets_router
from pipeline.shared import execution


LOGGER = logging.getLogger(__name__)


def create_app(
    data_directory: Path | None = None, *, gold_schema=None, cors_origins=None, gold_repository=None
) -> FastAPI:
    settings = Settings.load(data_directory, gold_schema, cors_origins)
    application = FastAPI(
        title="Viajando com o Dado",
        version="0.3.0",
        description="Dados publicados, qualidade, transformações e linhagem da pipeline.",
    )
    application.state.gold_repository = gold_repository or GoldRepository(settings.gold_schema)

    try:

        year = execution.current(settings.data_directory / "pipeline_status.json").get("year", "2025")
    except (json.JSONDecodeError, OSError):
        year = "2025"

    application.state.current_data = CurrentData(settings.data_directory, year, application.state.gold_repository)
    application.state.trace_store = TraceStore(schema_name=settings.gold_schema)
    application.state.trace_service = TraceService(application.state.current_data, application.state.trace_store)
    application.state.journey_service = JourneyService(application.state.trace_service)
    application.state.discovery = DiscoveryService(application.state.current_data)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET"],
        allow_headers=["Accept", "Content-Type"],
        expose_headers=["Content-Disposition", "ETag"],
    )

    @application.exception_handler(TraceUnavailableError)
    def integrity_error(request: Request, error: TraceUnavailableError):
        return JSONResponse(status_code=409, content={"detail": str(error)})

    @application.exception_handler(json.JSONDecodeError)
    def invalid_history(request: Request, error: json.JSONDecodeError):
        return JSONResponse(status_code=409, content={"detail": "Estado da execução inválido."})

    @application.exception_handler(FileNotFoundError)
    @application.exception_handler(DatasetNotFoundError)
    def missing_resource(request: Request, error: Exception):
        return JSONResponse(status_code=404, content={"detail": "Recurso não encontrado."})

    @application.exception_handler(ValueError)
    def invalid_parameter(request: Request, error: ValueError):
        return JSONResponse(status_code=422, content={"detail": str(error)})

    @application.exception_handler(DatasetUnavailableError)
    def pending_load(request: Request, error: DatasetUnavailableError):
        return JSONResponse(status_code=503, content={"detail": str(error)})

    @application.exception_handler(psycopg.Error)
    @application.exception_handler(duckdb.Error)
    @application.exception_handler(OSError)
    def unavailable(request: Request, error: Exception):
        LOGGER.warning("Consulta indisponível: %s", type(error).__name__)

        return JSONResponse(status_code=503, content={"detail": "Fonte de dados indisponível."})

    @application.get("/health")
    def health():
        return {"status": "ok", "tracking": "current"}

    application.include_router(datasets_router)
    application.include_router(pipeline_router)

    return application


app = create_app()
