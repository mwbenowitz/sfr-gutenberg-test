import requests
import os
import sys
import logging
from lxml import etree

from lib.gutenberg_xml import gutenbergXML
from lib.gutenberg_store import GutenbergDB
from lib.gutenberg_elastic import GutenbergES
from readers.metadatawrangler import MetadataWranglerReader
from readers.oclc import oclcReader

class GutenbergBib:
    def __init__(self, catalog_dir):
        self.logger = logging.getLogger('guten_logs')

        # This is where we read the RDF files from
        self.epub_dir = catalog_dir + "/cache/epub/"

        # These should get reset for next book
        self.currentBib = None
        self.metadata = {}
        self.ebookURLs = []

        self.gutenbergXML = gutenbergXML()
        self.mwReader = MetadataWranglerReader()
        self.oclcReader = oclcReader()

        self.dbConnector = GutenbergDB()
        self.esConnector = GutenbergES()

    # This is called after each work is processed to prep for the next
    def reset(self):
        self.currentBib = None
        self.metadata = {}
        self.ebookURLs = []
        self.gutenbergXML.metadata["entities"] = []
        self.gutenbergXML.metadata["subjects"] = []

    def read_dir(self):
        self.logger.info("Parsing all Gutenberg books")

        list(map(self.read_bib, os.listdir(self.epub_dir)))
        #list(map(self.read_bib, ["19540"]))
        self.dbConnector.closeAll()


    def read_bib(self, book_id):
        self.logger.debug("READING " + str(book_id))
        # Load the ebook URLs and book metadata
        status = self.load_bib(book_id)

        # If we failed to create the book warn and continue
        if status is False:
            self.logger.warning("DID NOT PARSE BOOK " + book_id)
            return False

        # Enhance the data we got from Gutenberg with data from MW
        self.enhance_bib()

        # Store the book in the database
        res = self.dbConnector.insert_record(self.metadata, self.ebookURLs)
        if res["result"] > 0:
            self.logger.error("WORK INSERT FAILED FOR {}".format(res["work"]))
            sys.exit(3)
        self.logger.debug("{} RECORD {}".format(res["status"], res["work"]))
        self.reset()

        self.esConnector.storeES(res["work"])

        return True

    # This provides the main processing for each work and loads metadata from it
    def load_bib(self, book_id):
        rdf_dir = self.epub_dir + book_id
        if 'DELETE' in book_id:
            self.logger.warning("BOOK TO BE DELETED")
            return False
        elif os.path.isdir(rdf_dir) is False or int(book_id) == 0:
            return False

        self.currentBib = book_id
        rdf_file = rdf_dir + "/" + os.listdir(rdf_dir)[0]

        self.metadata, self.ebookURLs = self.gutenbergXML.load(rdf_file)
        return True


    # Enhance records by querying MetadataWrangler and add any additional metadata
    # that we get back
    def enhance_bib(self):

        self.logger.info("Formatting Record")

        self.logger.info("Loading Metadata Wrangler Data")
        self.metadata = self.mwReader.getMWData(self.metadata, self.currentBib)

        self.logger.info("Loading OCLC Data")
        self.oclcReader.getOCLCData(self.metadata)
