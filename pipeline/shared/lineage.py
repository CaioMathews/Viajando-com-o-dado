"""Vínculos leves da publicação atual. Não arquiva dados, código ou versões anteriores."""

from datetime import datetime, timezone
from pathlib import Path
import json

import duckdb
from psycopg import sql
from psycopg.types.json import Jsonb
from pyspark.sql import functions as F

from pipeline.shared.catalog import catalog, find_node
from pipeline.shared import execution
from pipeline.shared.files import fingerprint, save_json
from pipeline.shared.record_keys import record_id, clean
from pipeline.shared.storage import write_parquet


def prepare(connection):
    connection.execute("CREATE SCHEMA IF NOT EXISTS pipeline_trace")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS pipeline_trace.current (singleton boolean PRIMARY KEY CHECK(singleton), payload jsonb NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS pipeline_trace.links (output_node text NOT NULL, output_id text NOT NULL, output_key jsonb NOT NULL, input_node text NOT NULL, input_id text NOT NULL, input_key jsonb NOT NULL, relation text NOT NULL, PRIMARY KEY(output_node,output_id,input_node,input_id,relation))"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS links_input_idx ON pipeline_trace.links(input_node,input_id)"
    )


def write_inmet_links(observations, output_directory, start_date, end_date):
    """Captura as chaves dos participantes usando os calendários já calculados pela Gold."""

    frames = []

    for date_column, relation in [("instant_date", "instant"), ("interval_date", "interval")]:

        frame = observations.filter(F.col(date_column).between(start_date, end_date)).select(
            "municipality_id",
            F.col(date_column).alias("observation_date"),
            "station_code",
            "timestamp_utc",
            F.col("_lineage_node").alias("input_node"),
            F.lit(relation).alias("relation"),
        )
        frames.append(frame)

    links = (
        frames[0]
        .unionByName(frames[1])
        .groupBy(
            "municipality_id", "observation_date", "station_code", "timestamp_utc", "input_node"
        )
        .agg(F.concat_ws(",", F.sort_array(F.collect_set("relation"))).alias("relation"))
    )
    destination = Path(output_directory).parents[2] / "lineage" / ("inmet_" + str(start_date.year))

    write_parquet(links, destination, "record_links", partition_by_state=False, track_output=False)
    save_json(
        destination.parent / (destination.name + ".json"),
        {"gold_files": fingerprint(output_directory)},
    )


def publish(connection, data_directory, year, schema_name="gold"):
    """Publica somente as relações não triviais, na mesma transação das tabelas da Gold."""

    data_directory = Path(data_directory)
    prepare(connection)
    connection.execute("DELETE FROM pipeline_trace.links")
    counts = {
        "municipalities": 0,
        "population": 0,
        "geocoding": 0,
        "open_meteo_daily": 0,
        "inmet_daily": 0,
    }

    def write_link(copy, output_node, output_key, input_node, input_key, relation):
        output_key = clean(output_key)
        input_key = clean(input_key)

        copy.write_row(
            (
                output_node,
                record_id(output_node, output_key),
                Jsonb(output_key),
                input_node,
                record_id(input_node, input_key),
                Jsonb(input_key),
                relation,
            )
        )

    # Produz os vínculos a partir das saídas atuais, sem duplicar os valores das tabelas.
    with duckdb.connect(":memory:", config={"threads": 1, "memory_limit": "1GB"}) as reader:

        with connection.cursor() as cursor:

            with cursor.copy(
                "COPY pipeline_trace.links (output_node,output_id,output_key,input_node,input_id,input_key,relation) FROM STDIN"
            ) as copy:

                for dataset in [
                    "municipalities",
                    "population",
                    "geocoding",
                    "open_meteo_daily",
                    "inmet_daily",
                ]:

                    node = find_node("gold:" + dataset, year)
                    paths = []

                    for path in sorted((data_directory / node["path"]).rglob("*.parquet")):

                        paths.append(str(path))

                    if not paths:
                        raise FileNotFoundError("Gold ausente: " + dataset)

                    columns = ", ".join(node["key_fields"])

                    if dataset == "open_meteo_daily":
                        columns += ", source_record_present"

                    result = reader.execute(
                        "SELECT "
                        + columns
                        + " FROM read_parquet(?, hive_partitioning=false, union_by_name=true)",
                        [paths],
                    )

                    names = []

                    for column in result.description:

                        names.append(column[0])

                    while True:

                        batch = result.fetchmany(2000)

                        if not batch:
                            break

                        for values in batch:

                            row = dict(zip(names, values))
                            key = {}

                            for field in node["key_fields"]:

                                key[field] = row[field]

                            counts[dataset] += 1

                            if dataset != "inmet_daily" and (
                                dataset != "open_meteo_daily" or row["source_record_present"]
                            ):
                                write_link(
                                    copy, node["id"], key, "silver:" + dataset, key, "transform"
                                )

                            if dataset in {"population", "geocoding"}:
                                write_link(
                                    copy,
                                    node["id"],
                                    key,
                                    "gold:municipalities",
                                    {"municipality_id": row["municipality_id"]},
                                    "official_identity",
                                )

                            if dataset in {"open_meteo_daily", "inmet_daily"}:
                                write_link(
                                    copy,
                                    node["id"],
                                    key,
                                    "gold:geocoding",
                                    {"municipality_id": row["municipality_id"]},
                                    "capital_location",
                                )

                directory = data_directory / "lineage" / ("inmet_" + str(year))

                manifest = directory.parent / (directory.name + ".json")
                expected = json.loads(manifest.read_text(encoding="utf-8"))

                gold_path = data_directory / find_node("gold:inmet_daily", year)["path"]

                if expected["gold_files"] != fingerprint(gold_path):
                    raise ValueError(
                        "Os vínculos do INMET não correspondem à Gold atual. Execute a Gold antes do Load."
                    )

                paths = []

                for path in sorted(directory.rglob("*.parquet")):

                    paths.append(str(path))

                result = reader.execute(
                    "SELECT municipality_id, observation_date, station_code, timestamp_utc, input_node, relation FROM read_parquet(?, hive_partitioning=false)",
                    [paths],
                )

                while True:

                    batch = result.fetchmany(2000)

                    if not batch:
                        break

                    for municipality, day, station, timestamp, parent, relation in batch:

                        write_link(
                            copy,
                            "gold:inmet_daily",
                            {"municipality_id": municipality, "observation_date": day},
                            parent,
                            {"station_code": station, "timestamp_utc": timestamp},
                            relation,
                        )

    state = execution.active_run() or execution.current()
    files = {}

    for node in catalog(year):

        if node["layer"] in {"bronze", "silver", "gold"}:
            files[node["id"]] = fingerprint(data_directory / node["path"])

        elif node["layer"] == "source":
            files[node["id"]] = fingerprint(
                data_directory / node["path"], {".csv", ".json", ".xls", ".xlsx"}
            )

    payload = {
        "run_id": state["run_id"],
        "schema_name": schema_name,
        "year": str(year),
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "files": files,
    }

    connection.execute(
        "INSERT INTO pipeline_trace.current VALUES (true,%s) ON CONFLICT(singleton) DO UPDATE SET payload=EXCLUDED.payload",
        (Jsonb(payload),),
    )
