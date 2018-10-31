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

    def __init__(self, catalogDir):
        self.logger = logging.getLogger('guten_logs')

        # This is where we read the RDF files from
        self.epubDir = catalogDir + "/cache/epub/"

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

    def readDir(self):
        self.logger.info("Parsing all Gutenberg books")

        list(map(self.readBib, os.listdir(self.epubDir)))
        self.dbConnector.closeAll()
        self.esConnector.closeConn()


    def readBib(self, bookID):
        self.logger.info("READING {}".format(bookID))
        # Load the ebook URLs and book metadata
        status = self.loadBib(bookID)

        # If we failed to create the book warn and continue
        if status is False:
            self.logger.warning("DID NOT PARSE BOOK {}".format(bookID))
            return False

        # Enhance the data we got from Gutenberg with data from MW
        enhanceStatus = self.enhanceBib()
        if enhanceStatus is not True:
            return ehanceStatus

        # Store the book in the database
        res = self.dbConnector.insert_record(self.metadata, self.ebookURLs)
        if res["result"] > 0:
            self.logger.error("WORK INSERT FAILED FOR {}".format(res["work"]))
            sys.exit(3)
        self.logger.debug("{} RECORD {}".format(res["status"], res["work"]))
        self.reset()

        if self.test is True:
            return True

        if res["status"] == "existing":
            self.esConnector.dropES(res["work"])
        self.esConnector.storeES(res["work"])
        return True

    # This provides the main processing for each work and loads metadata from it
    def loadBib(self, bookID):
        rdfDir = "{}{}".format(self.epubDir, bookID)
        if 'DELETE' in str(bookID):
            # TODO Execute the delete request in the psql/es
            self.logger.warning("BOOK TO BE DELETED")
            return False
        elif os.path.isdir(rdfDir) is False or int(bookID) == 0:
            return False

        self.currentBib = bookID
        rdfFile = "{}/{}".format(rdfDir, os.listdir(rdfDir)[0])
        self.logger.info("Loading publication from {}".format(rdfFile))
        self.metadata, self.ebookURLs = self.gutenbergXML.load(rdfFile)
        return True


    # Enhance records by querying MetadataWrangler and add any additional metadata
    # that we get back
    def enhanceBib(self):

        self.logger.info("Formatting Record")

        self.logger.info("Loading Metadata Wrangler Data")
        self.metadata = self.mwReader.getMWData(self.metadata, self.currentBib)
        if "ids" not in self.metadata:
            self.logger.warning("BAD RECORD. CHECK SOURCE GUTENBERG FILE")
            return False
        self.logger.info("Loading OCLC Data")
        self.oclcReader.getOCLCData(self.metadata, self.currentBib)

        return True
