"""Configuração local compartilhada pelo backend, sem expor credenciais nas respostas."""

from dataclasses import dataclass
import os
from pathlib import Path
import re

from dotenv import load_dotenv


PROJECT_DIRECTORY = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    data_directory: Path
    gold_schema: str
    cors_origins: list[str]

    @classmethod
    def load(cls, data_directory=None, gold_schema=None, cors_origins=None):
        load_dotenv(PROJECT_DIRECTORY / ".env", override=False, interpolate=False)
        schema = gold_schema or os.environ.get("VCD_GOLD_SCHEMA", "gold")

        if (
            re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema) is None
            or schema.startswith("pg_")
            or schema == "information_schema"
        ):
            raise ValueError("Schema Gold inválido na configuração do backend.")

        root = data_directory or os.environ.get("VCD_DATA_DIRECTORY") or PROJECT_DIRECTORY / "data"
        origins = cors_origins

        if origins is None:
            origins = []

            for origin in os.environ.get(
                "VCD_CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
            ).split(","):

                if origin.strip():
                    origins.append(origin.strip())

        return cls(Path(root).resolve(), schema, origins)
