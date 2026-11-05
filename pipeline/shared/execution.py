"""Acompanha somente a execução atual: estado, duração, quantidade e destino de cada etapa."""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
import inspect
import json
import os
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from pipeline.shared.files import save_json


PROJECT_DIRECTORY = Path(__file__).resolve().parents[2]
STATUS_PATH = PROJECT_DIRECTORY / "data/pipeline_status.json"
_RUN = ContextVar("pipeline_execution", default=None)
_STEP = ContextVar("pipeline_step", default=None)


def now():
    return datetime.now(timezone.utc).isoformat()


def current(path=None):
    selected = Path(path or STATUS_PATH)

    if not selected.exists():
        return {"run_id": None, "status": "not_started", "year": "2025", "nodes": {}}

    return json.loads(selected.read_text(encoding="utf-8"))


def safe_error(error):
    try:

        message = str(error)
    except Exception:
        message = type(error).__name__

    password = os.environ.get("PGPASSWORD")

    if password:
        message = message.replace(password, "[redigido]")

    return {"type": type(error).__name__, "message": message[:2000]}


@contextmanager
def run_context(project_directory=None, parameters=None):
    if os.environ.get("VCD_TRACKING_ENABLED", "1") == "0":
        yield None

        return

    if _RUN.get() is not None:
        yield _RUN.get()

        return

    state = {"run_id": uuid4().hex, "status": "running", "year": str((parameters or {}).get("year", "2025")), "started_at": now(), "finished_at": None, "nodes": {}}
    token = _RUN.set(state)

    try:

        save_json(STATUS_PATH, state)

        try:

            yield state
        except BaseException as error:
            state.update(status="failed", error=safe_error(error))

            raise
        else:
            if state["status"] != "failed":
                state["status"] = "succeeded"
        finally:
            state["finished_at"] = now()
            save_json(STATUS_PATH, state)
    finally:
        _RUN.reset(token)


def pipeline_execution(function):
    @wraps(function)
    def wrapped(*arguments, **options):
        bound = inspect.signature(function).bind(*arguments, **options)
        bound.apply_defaults()

        with run_context(parameters=bound.arguments):

            return function(*arguments, **options)

    return wrapped


def step(layer, source, before_inputs=None):
    datasets = {"ibge_municipalities": "municipalities", "ibge_population": "population", "geocoding": "geocoding", "open_meteo": "open_meteo_daily", "inmet": "inmet_daily", "postgres": "postgres", "consume": "consume"}

    def decorate(function):
        @wraps(function)
        def wrapped(*arguments, **options):
            bound = inspect.signature(function).bind(*arguments, **options)
            bound.apply_defaults()
            parameters = bound.arguments

            with run_context(parameters=parameters) as state:

                if state is None:
                    if before_inputs:
                        before_inputs(parameters)

                    return function(*arguments, **options)

                dataset = datasets[source]
                directory = parameters.get("output_directory") or parameters.get("input_directory")

                if source == "inmet" and directory and Path(directory).name.startswith("boundary_"):
                    dataset = "inmet_boundary"

                node_id = layer + ":" + dataset
                item = {"status": "running", "started_at": now(), "function": function.__name__, "file": str(Path(inspect.getsourcefile(function)).relative_to(PROJECT_DIRECTORY)), "records": None}
                state["nodes"][node_id] = item
                token = _STEP.set(item)

                try:

                    started = perf_counter()
                    save_json(STATUS_PATH, state)

                    try:

                        if before_inputs:
                            before_inputs(parameters)

                        result = function(*arguments, **options)

                        if layer == "consume" and result:
                            raise ValueError("Coleta incompleta nas UFs: " + ", ".join(result))

                        item["status"] = "succeeded"

                        return result
                    except BaseException as error:
                        item.update(status="failed", error=safe_error(error))
                        state["status"] = "failed"

                        raise
                    finally:
                        item.update(finished_at=now(), duration_seconds=round(perf_counter() - started, 3))
                        save_json(STATUS_PATH, state)
                finally:
                    _STEP.reset(token)

        return wrapped

    return decorate


def set_output(path, records):
    item = _STEP.get()

    if item is not None:
        selected = Path(path).resolve()
        relative = selected.relative_to(PROJECT_DIRECTORY) if selected.is_relative_to(PROJECT_DIRECTORY) else selected
        item.update(path=str(relative), records=records)


def active_run():
    return _RUN.get()


def finish_notebook(session, spark):
    try:

        spark.stop()
    except BaseException as error:
        session.__exit__(type(error), error, error.__traceback__)

        raise

    session.__exit__(None, None, None)
