
import unittest
import os

from helpers.postgres import postgresManager

class TestGutenbergStack(unittest.TestCase):

    def setup(self):


    def test_files_existence(self):
        self.assertTrue(os.path.isdir(os.path.dirname(__file__) + "/../files"))

    def test_config_exstince(self):
        self.assertTrue(os.path.isfile(os.path.dirname(__file__) + "../gutenberg.conf"))

    def test_db_status(self):
        pass

class TestGutenbergDB(unittest.TestCase):

if __name__ == '__main__':
    unittest.main()
