
import unittest
import os

from main import ingest_full_gutenberg as ingest_full

class TestFullIngest(unittest.TestCase):
    def test_files_existence(self):
        self.assertTrue(os.path.isdir(os.path.dirname(__file__) + "/../files"))

    def test_config_exstince(self):
        self.assertTrue(os.path.isfile(os.path.dirname(__file__) + "../gutenberg.conf"))

    

if __name__ == '__main__':
    unittest.main()
