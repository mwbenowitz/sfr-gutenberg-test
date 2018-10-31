
import unittest
import os
from sqlalchemy import create_engine
import psycopg2

from helpers.postgres import postgresManager
from helpers.elasticsearch import ElasticWriter

from lib.gutenberg_parse import GutenbergBib

class TestGutenbergStack(unittest.TestCase):

    def setUp(self):
        self.psql = postgresManager(test=True)
        self.tables = [
            "works",
            "instances",
            "items",
            "subjects",
            "entities",
            "identifiers",
            "work_identifiers",
            "instance_identifiers",
            "item_identifiers",
            "subject_works",
            "entity_works",
            "entity_instances",
            "entity_items"
        ]

        self.esWriter = ElasticWriter()

    def test_files_existence(self):
        self.assertTrue(os.path.isdir(os.path.dirname(__file__) + "/../files"))

    def test_config_exstince(self):
        self.assertTrue(os.path.isfile(os.path.dirname(__file__) + "/../gutenberg.conf"))

    # Check if tables exist
    def test_db_status(self):
        self.psql.getCursor()
        self.psql.cursor.execute("SELECT table_name from information_schema.tables")
        tables = self.psql.cursor.fetchall()
        tableList = [table["table_name"] for table in tables]
        self.assertTrue(len(list(set(tableList) & set(self.tables))) == len(self.tables))

    def test_es_status(self):
        esResp = self.esWriter.es.search()
        self.assertFalse(esResp["timed_out"])
        self.assertEqual(esResp["_shards"]["total"], 5)
        self.assertTrue("hits" in esResp)

    def tearDown(self):
        self.psql.closeAll()

class TestGutenberg(unittest.TestCase):

    def setUp(self):
        self.psql = postgresManager(test=True)
        self.pgWriter = GutenbergBib("files/gutenberg_catalog", test=True)

    def test_db_insert(self):
        self.assertTrue(self.pgWriter.readBib("10065"))

    def test_db_insert_err(self):
        self.assertFalse(self.pgWriter.readBib("0"))

    def test_record_load(self):
        self.assertTrue(self.pgWriter.loadBib("1034"))
        # This will enhance the record loaded above
        self.assertTrue(self.pgWriter.enhanceBib())

    def tearDown(self):
        self.psql.closeAll()



if __name__ == '__main__':
    unittest.main()
