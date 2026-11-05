"""Confere que o Load inclui os vínculos horários INMET, além das dimensões."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import duckdb

from pipeline.shared import lineage
from pipeline.shared.catalog import find_node
from pipeline.shared.files import fingerprint


class LineagePublicationTests(unittest.TestCase):
    def test_inmet_batch_is_read_and_published(self):
        with tempfile.TemporaryDirectory() as directory:

            data = Path(directory)

            with duckdb.connect() as writer:

                for dataset in [
                    "municipalities",
                    "population",
                    "geocoding",
                    "open_meteo_daily",
                    "inmet_daily",
                ]:

                    path = data / find_node("gold:" + dataset, "2025")["path"]
                    path.mkdir(parents=True)
                    fields = "1302603::BIGINT AS municipality_id"

                    if dataset == "population":
                        fields += ", DATE '2025-07-01' AS reference_date"

                    if dataset in {"open_meteo_daily", "inmet_daily"}:
                        fields += ", DATE '2025-01-01' AS observation_date"

                    if dataset == "open_meteo_daily":
                        fields += ", true AS source_record_present"

                    writer.execute(
                        "COPY (SELECT " + fields + ") TO ? (FORMAT PARQUET)",
                        [str(path / "rows.parquet")],
                    )

                links = data / "lineage/inmet_2025"
                links.mkdir(parents=True)
                writer.execute(
                    "COPY (SELECT 1302603::BIGINT AS municipality_id, DATE '2025-01-01' AS observation_date, 'A101' AS station_code, TIMESTAMP '2025-01-01 04:00:00' AS timestamp_utc, 'silver:inmet_daily' AS input_node, 'instant' AS relation) TO ? (FORMAT PARQUET)",
                    [str(links / "rows.parquet")],
                )

            manifest = {
                "gold_files": fingerprint(data / find_node("gold:inmet_daily", "2025")["path"])
            }
            (links.parent / "inmet_2025.json").write_text(json.dumps(manifest), encoding="utf-8")
            connection = MagicMock()
            copy = (
                connection.cursor.return_value.__enter__.return_value.copy.return_value.__enter__.return_value
            )

            with patch.object(lineage.execution, "active_run", return_value={"run_id": "test"}):

                lineage.publish(connection, data, "2025")

            matches = []

            for call in copy.write_row.call_args_list:

                row = call.args[0]

                if row[0] == "gold:inmet_daily" and row[3] == "silver:inmet_daily":
                    matches.append(row)

            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0][-1], "instant")
