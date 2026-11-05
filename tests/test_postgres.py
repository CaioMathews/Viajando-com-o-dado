from datetime import date
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from pipeline.load import postgres


class PostgresConfigurationTests(unittest.TestCase):
    def test_missing_configuration_does_not_connect(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(postgres, 'load_environment'), patch.object(postgres.psycopg, 'connect') as connect:

            with self.assertRaises(ValueError):

                postgres.get_connection()

            connect.assert_not_called()

    def test_unsafe_schema_is_rejected(self):
        for name in ['gold; DROP SCHEMA public', 'pg_catalog', 'information_schema', 'Gold']:
            with self.subTest(name=name), self.assertRaises(ValueError):

                postgres.validate_identifier(name)

    def test_environment_loading_preserves_existing_values(self):
        with patch.object(postgres, 'load_dotenv') as loader:

            postgres.load_environment()

        loader.assert_called_once_with(ROOT / '.env', override=False, interpolate=False)

    def test_existing_database_is_not_recreated(self):
        with patch.dict(os.environ, {'PGDATABASE': 'viajando_com_o_dado'}), patch.object(postgres, 'load_environment'), patch.object(postgres, 'get_connection') as connect:

            cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
            cursor.fetchone.return_value = (True,)
            self.assertFalse(postgres.create_database())
            connect.assert_called_once_with(database_name='postgres', autocommit=True)
            self.assertEqual(cursor.execute.call_count, 1)

    def test_nested_fields_use_jsonb(self):
        fields = [StructField('municipality_id', LongType()), StructField('sources', ArrayType(StringType()))]
        frame = SimpleNamespace(schema=StructType(fields), columns=['municipality_id', 'sources'])
        self.assertEqual(postgres.get_column_types(frame), {'municipality_id': 'bigint', 'sources': 'jsonb'})

    def test_invalid_year_is_rejected_before_reading(self):
        with self.assertRaises(ValueError):

            postgres.read_gold_tables(None, ROOT / 'data/gold', '../2025')


@unittest.skipUnless(os.environ.get('TEST_POSTGRES_DSN'), 'Requer PostgreSQL exclusivo para testes em TEST_POSTGRES_DSN.')
class PostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        builder = SparkSession.builder.master('local[2]').appName('Testes Load PostgreSQL').config('spark.sql.shuffle.partitions', '2')

        if os.name == 'nt':
            jar = ROOT / 'pipeline/bronze/lib/hadoop-bare-naked-local-fs-0.1.0.jar'
            builder = builder.config('spark.driver.extraClassPath', str(jar)).config('spark.executor.extraClassPath', str(jar)).config('spark.hadoop.fs.file.impl', 'com.globalmentor.apache.hadoop.fs.BareLocalFileSystem')

        cls.spark = builder.getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        environment = patch.dict(os.environ, {"VCD_TRACKING_ENABLED": "0"})
        environment.start()
        self.addCleanup(environment.stop)
        self.schema = 'test_load_' + uuid4().hex
        self.connect_patch = patch.object(postgres, 'get_connection', side_effect=self.connection)
        self.connect_patch.start()
        self.addCleanup(self.connect_patch.stop)
        self.addCleanup(self.drop_schema)

    def connection(self):
        return psycopg.connect(os.environ['TEST_POSTGRES_DSN'])

    def drop_schema(self):
        with self.connection() as connection:

            connection.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(self.schema)))

    def test_database_creation_is_idempotent(self):
        database_name = 'test_database_' + uuid4().hex

        def admin_connection(**options):
            return psycopg.connect(os.environ['TEST_POSTGRES_DSN'], autocommit=True)

        try:

            with patch.dict(os.environ, {'PGDATABASE': database_name}), patch.object(postgres, 'load_environment'), patch.object(postgres, 'get_connection', side_effect=admin_connection):

                self.assertTrue(postgres.create_database())
                self.assertFalse(postgres.create_database())

            with psycopg.connect(os.environ['TEST_POSTGRES_DSN'], dbname=database_name) as connection:

                self.assertEqual(connection.execute('SELECT current_database()').fetchone()[0], database_name)
        finally:
            with psycopg.connect(os.environ['TEST_POSTGRES_DSN'], autocommit=True) as connection:

                connection.execute(sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(database_name)))

    def test_current_gold_round_trip_updates_and_rollback(self):
        if not os.environ.get('TEST_GOLD_DIRECTORY'):
            self.skipTest('Configure TEST_GOLD_DIRECTORY para testar as cinco saídas reais.')

        directory = Path(os.environ['TEST_GOLD_DIRECTORY'])
        tables = postgres.read_gold_tables(self.spark, directory, '2025')

        with patch.object(postgres, 'read_gold_tables', return_value=tables):

            first = postgres.load_gold(self.spark, directory, schema_name=self.schema)
            second = postgres.load_gold(self.spark, directory, schema_name=self.schema)
            self.assertEqual(first, second)
            self.assertEqual(first['municipalities'], 27)
            self.assertEqual(first['geocoding'], 27)
            self.assertEqual(first['open_meteo_daily'], 9855)
            self.assertEqual(first['inmet_daily'], 9855)

            with self.connection() as connection:

                for name, expected in first.items():
                    count = connection.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier(self.schema, name))).fetchone()[0]
                    self.assertEqual(count, expected)

                origin = connection.execute(sql.SQL('SELECT source_files FROM {} LIMIT 1').format(sql.Identifier(self.schema, 'municipalities'))).fetchone()[0]
                self.assertIsInstance(origin, list)
                original = connection.execute(sql.SQL('SELECT name FROM {} WHERE municipality_id = 3550308').format(sql.Identifier(self.schema, 'municipalities'))).fetchone()[0]

            actual_load = postgres.load_table

            def fail_last(connection, dataframe, table_name, schema_name):
                if table_name == 'municipalities':
                    connection.execute(sql.SQL('UPDATE {} SET name = %s WHERE municipality_id = 3550308').format(sql.Identifier(schema_name, table_name)), ('Falha simulada',))

                    return first[table_name]

                if table_name == 'inmet_daily':
                    raise ValueError('Falha simulada na última tabela')

                return actual_load(connection, dataframe, table_name, schema_name)

            with patch.object(postgres, 'load_table', side_effect=fail_last), self.assertRaises(ValueError):

                postgres.load_gold(self.spark, directory, schema_name=self.schema)

            with self.connection() as connection:

                actual = connection.execute(sql.SQL('SELECT name FROM {} WHERE municipality_id = 3550308').format(sql.Identifier(self.schema, 'municipalities'))).fetchone()[0]
                self.assertEqual(actual, original)

    def test_upsert_preserves_other_dates_and_json_null_fields(self):
        catalog = self.spark.createDataFrame([(3550308, 'São Paulo')], 'municipality_id long, name string')
        population = self.spark.createDataFrame([(3550308, date(2024, 7, 1), 10)], 'municipality_id long, reference_date date, population_estimate long')
        updated = self.spark.createDataFrame([(3550308, date(2025, 7, 1), 20)], population.schema)
        replaced = self.spark.createDataFrame([(3550308, date(2025, 7, 1), 30)], population.schema)
        coordinates = self.spark.createDataFrame([(3550308, ('origem', None))], 'municipality_id long, source struct<file:string,note:string>')

        with self.connection() as connection:

            connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
            postgres.load_table(connection, catalog, 'municipalities', self.schema)
            postgres.load_table(connection, population, 'population', self.schema)
            postgres.load_table(connection, updated, 'population', self.schema)
            postgres.load_table(connection, replaced, 'population', self.schema)
            postgres.load_table(connection, coordinates, 'geocoding', self.schema)

        with self.connection() as connection:

            rows = connection.execute(sql.SQL('SELECT population_estimate FROM {} ORDER BY reference_date').format(sql.Identifier(self.schema, 'population'))).fetchall()
            self.assertEqual(rows, [(10,), (30,)])
            origin = connection.execute(sql.SQL('SELECT source FROM {}').format(sql.Identifier(self.schema, 'geocoding'))).fetchone()[0]
            self.assertEqual(origin, {'file': 'origem', 'note': None})

    def test_duplicates_are_rejected(self):
        frame = self.spark.createDataFrame([(3550308,), (3550308,)], 'municipality_id long')

        with self.connection() as connection, self.assertRaises(ValueError):

            postgres.load_table(connection, frame, 'municipalities', self.schema)

    def test_nonfinite_value_rolls_back_table_creation(self):
        frame = self.spark.createDataFrame([(3550308, float('inf'))], 'municipality_id long, value double')

        with self.assertRaises(ValueError), self.connection() as connection:

            connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
            postgres.load_table(connection, frame, 'municipalities', self.schema)

        with self.connection() as connection:

            self.assertIsNone(connection.execute('SELECT to_regnamespace(%s)', (self.schema,)).fetchone()[0])


if __name__ == '__main__':
    unittest.main()
