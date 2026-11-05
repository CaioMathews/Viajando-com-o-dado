"""Conexões curtas e somente de leitura para as consultas do backend."""

import os

import psycopg
from psycopg import IsolationLevel


def get_connection():
    connection = psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE"),
        user=os.environ.get("PGUSER"),
        password=os.environ.get("PGPASSWORD"),
        sslmode=os.environ.get("PGSSLMODE", "prefer"),
        connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=5000",
        application_name="viajando_backend_reader",
    )

    # Resumos e metadados com várias consultas enxergam a mesma versão da carga.
    connection.isolation_level = IsolationLevel.REPEATABLE_READ

    return connection
