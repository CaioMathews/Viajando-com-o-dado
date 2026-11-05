from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import os
import subprocess
import sys

import duckdb
from fastapi.testclient import TestClient

from backend.main import create_app
from backend.repositories.current_data import CurrentData
from backend.services.search import resolve, suggestions
from backend.services.trace import TraceService, TraceUnavailableError
from pipeline.shared import execution
from pipeline.shared.catalog import catalog
from pipeline.shared.lineage import fingerprint
from pipeline import run_pipeline as runner


CAPITALS = [
    {'name': 'Recife', 'state_name': 'Pernambuco', 'state_code': 'PE', 'municipality_id': 2611606},
    {'name': 'São Paulo', 'state_name': 'São Paulo', 'state_code': 'SP', 'municipality_id': 3550308},
    {'name': 'São Luís', 'state_name': 'Maranhão', 'state_code': 'MA', 'municipality_id': 2111300},
]


class CurrentPipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.reader = CurrentData(self.directory)

    def parquet(self, node_id, query):
        directory = self.directory / self.reader.node(node_id)['path']
        directory.mkdir(parents=True, exist_ok=True)

        with duckdb.connect() as connection:

            connection.execute('COPY (' + query + ') TO ? (FORMAT PARQUET)', [str(directory / 'data.parquet')])

    def test_collection_cli_bootstraps_the_project_without_pythonpath(self):

        script = Path(__file__).resolve().parents[1] / 'pipeline/consume APIs/consume.py'
        environment = os.environ.copy()
        environment.pop('PYTHONPATH', None)
        result = subprocess.run([sys.executable, '-B', str(script), '--help'], cwd=self.directory, env=environment, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--year', result.stdout)

    def test_collection_subprocess_keeps_the_parent_execution(self):

        status = self.directory / 'status.json'

        def child(*args, **options):
            self.assertEqual(options['env']['VCD_TRACKING_ENABLED'], '0')
            self.assertEqual(execution.current()['nodes']['consume:consume']['status'], 'running')

        with patch.dict(os.environ, {'VCD_TRACKING_ENABLED': '1'}), patch.object(execution, 'STATUS_PATH', status), patch.object(runner.subprocess, 'run', side_effect=child):

            runner.run_consume(Path(__file__).resolve().parents[1], '2025')
            state = execution.current()

        self.assertEqual(state['status'], 'succeeded')
        self.assertEqual(list(state['nodes']), ['consume:consume'])

    def test_typo_accents_partial_names_and_ambiguity(self):
        for query in ['recfe', 'RECIFE', 'PE', 'Pernambuco']:
            self.assertEqual(resolve(query, CAPITALS)['filters']['municipality_id'], 2611606)

        self.assertEqual(resolve('sao paulo', CAPITALS)['filters']['municipality_id'], 3550308)
        self.assertEqual(resolve('sao palo', CAPITALS)['filters']['municipality_id'], 3550308)
        ambiguous = resolve('sao', CAPITALS)
        self.assertEqual(ambiguous['filters'], {})
        self.assertEqual(len(ambiguous['suggestions']), 2)
        combined = resolve('sao; data=2025-01-01', CAPITALS)
        selected = resolve(combined['suggestions'][0]['query'], CAPITALS)
        self.assertEqual(selected['filters']['observation_date'], '2025-01-01')
        self.assertEqual(suggestions('recfe', CAPITALS)[0]['name'], 'Recife')

    def test_strict_ids_dates_and_combined_filters(self):
        result = resolve('recfe; data=01/01/2025', CAPITALS)
        self.assertEqual(result['filters'], {'municipality_id': 2611606, 'observation_date': '2025-01-01'})

        for query in ['municipality_id=1', '355030', 'data=2025-02-30', 'drop table=1']:
            with self.assertRaises(ValueError):

                resolve(query, CAPITALS)

    def test_current_rows_pagination_and_unsafe_columns(self):
        self.parquet('silver:municipalities', "SELECT 2611606 AS municipality_id, 'Recife' AS name, 'PE' AS state_code UNION ALL SELECT 3550308, 'São Paulo', 'SP'")
        self.assertEqual(self.reader.metadata('silver:municipalities')['records'], 2)
        page = self.reader.records('silver:municipalities', limit=1)
        self.assertTrue(page['has_more'])
        next_page = self.reader.records('silver:municipalities', offset=page['next_offset'], limit=1)
        self.assertFalse(next_page['has_more'])
        self.assertNotEqual(page['items'][0]['key'], next_page['items'][0]['key'])
        self.assertEqual(self.reader.records('silver:municipalities', {'field': 'name', 'value': 'sao'})['items'][0]['values']['name'], 'São Paulo')

        with self.assertRaises(ValueError):

            self.reader.records('silver:municipalities', {'field': 'name; DROP TABLE records', 'value': 'x'})

    def test_population_forward_path_checks_original_worksheet_row(self):
        self.parquet('silver:population', "SELECT 2611606 AS municipality_id, DATE '2025-07-01' AS reference_date, [{'source_file': 'population.xls', 'source_sheet': 'A', 'source_row_number': 12}] AS sources")
        page = self.reader.records('silver:population', source_contains={'source_file': 'population.xls', 'source_sheet': 'A', 'source_row_number': 12})
        self.assertEqual(len(page['items']), 1)
        absent = self.reader.records('silver:population', source_contains={'source_file': 'population.xls', 'source_sheet': 'A', 'source_row_number': 13})
        self.assertFalse(absent['items'])

    def test_stale_files_and_failed_execution_block_trace(self):
        self.parquet('silver:municipalities', "SELECT 2611606 AS municipality_id")
        service = TraceService(self.reader, None)
        node_id = 'silver:municipalities'
        directory = self.directory / self.reader.node(node_id)['path']
        state = {'run_id': 'current', 'status': 'succeeded'}
        published = {'run_id': 'current', 'files': {node_id: fingerprint(directory)}}

        with patch.object(execution, 'current', return_value=state):

            service.ensure_current(node_id, published)
            published['files'][node_id] = []

            with self.assertRaises(TraceUnavailableError):

                service.ensure_current(node_id, published)

        with patch.object(execution, 'current', return_value={**state, 'status': 'failed'}):

            with self.assertRaises(TraceUnavailableError):

                service.ensure_current(node_id, published)

    def test_graph_contains_real_joins_and_boundary(self):
        nodes = {}

        for node in catalog():
            nodes[node['id']] = node

        self.assertEqual(len(nodes), 28)
        self.assertEqual(set(nodes['gold:inmet_daily']['parents']), {'silver:inmet_daily', 'silver:inmet_boundary', 'gold:geocoding'})
        self.assertIn('gold:municipalities', nodes['gold:population']['parents'])

        for node in nodes.values():
            for parent in node['parents']:
                self.assertLessEqual(nodes[parent]['column'], node['column'])

                if nodes[parent]['column'] == node['column']:
                    self.assertLess(nodes[parent]['row'], node['row'])

    def test_api_contract_and_raw_file_access_are_restricted(self):
        with TestClient(create_app(self.directory)) as client:

            paths = client.get('/openapi.json').json()['paths']
            self.assertIn('/pipeline/journey', paths)

            for path in paths:
                self.assertNotIn('/audit', path)

            self.assertEqual(client.get('/pipeline/file', params={'path': '../.env'}).status_code, 404)
            self.assertEqual(client.get('/pipeline/journey', params={'node_id': 'gold:municipalities', 'key': '[]'}).status_code, 422)
            self.assertEqual(client.get('/pipeline/nodes/unknown').status_code, 422)


if __name__ == '__main__':
    unittest.main()
