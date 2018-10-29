import logging
import time

from lib.gutenberg_downloads import GutenbergDownloads
from lib.gutenberg_parse import GutenbergBib
from lib.gutenberg_store import GutenbergDB

class GutenbergCore:
    def __init__(self):
        self.logger = logging.getLogger("guten_logs")
        self.downloads = GutenbergDownloads()
        self.bib_parser = GutenbergBib(self.downloads.catalog_dir)
        self.data_storer = GutenbergDB()

    def ingest_gutenberg(self):
        self.logger.debug("Running normal ingest")

    def ingest_full_gutenberg(self):
        self.logger.debug("Running full ingest")

        # Download full RDF catalog from Gutenberg if it is more than 24 hours old
        self.downloads.get_rdf_records()

        self.bib_parser.read_dir()
