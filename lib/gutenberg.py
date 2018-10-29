import logging
import time

from lib.gutenberg_downloads import GutenbergDownloads
from lib.gutenberg_parse import GutenbergBib
from lib.gutenberg_store import GutenbergDB

class GutenbergCore:
    def __init__(self):
        self.logger = logging.getLogger("guten_logs")
        self.downloads = GutenbergDownloads()
        self.bibParser = GutenbergBib(self.downloads.catalogDir)

    def ingest_gutenberg(self):
        self.logger.debug("Running normal ingest")

    def ingest_full_gutenberg(self):
        self.logger.debug("Running full ingest")

        # Download full RDF catalog from Gutenberg if it is more than 24 hours old
        self.downloads.getRDFRecords()

        # Read the current directory of Gutenberg records and store them
        self.bibParser.readDir()
