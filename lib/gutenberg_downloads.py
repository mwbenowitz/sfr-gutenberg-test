import requests
import os
import shutil
import sys
import logging
import time
import tarfile

class GutenbergDownloads:

    gutenbergCatalog = "https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2"
    fileDir = "files/"
    catalogDir = fileDir + "gutenberg_catalog"

    def __init__(self):
        self.logger = logging.getLogger("guten_logs")

        self.runTime = time.time()
        self.cutoffTime = self.runTime - 86400

    def getRDFRecords(self):
        self.logger.debug("Checking time of last RDF Catalog Download")
        # Check if the path exists and if this was run in the past 24 hours
        catalogCheck = self._createCat()
        if catalogCheck is False:
            catalogModified = os.path.getmtime(GutenbergDownloads.catalogDir)
            if self.cutoffTime < catalogModified:
                self.logger.debug("RDF Catalog is current, no action is necessary")
                return False

        self.logger.info("RDF Catalog is expired, downloading a fresh copy")
        # If not, get the tar from Gutenberg
        self.logger.info("Deleting existing copy")
        shutil.rmtree(GutenbergDownloads.catalogDir)
        gutenberg_zip = requests.get(GutenbergDownloads.gutenbergCatalog)
        if gutenberg_zip.status_code != 200:
            self.logger.error("COULD NOT DOWNLOAD RDF FILE! EXITING")
            sys.exit(2)

        self.logger.debug("Unziping RDF tar.bz2...")
        tmpFile = GutenbergDownloads.fileDir + "gutenberg-rdf.tar.bz2"
        opnFile = open(tmpFile, "wb")
        opnFile.write(gutenberg_zip.content)
        opnFile.close()
        gutenbergTar = tarfile.open(tmpFile, "r:bz2")
        self.logger.debug("Extracting to a directory...")
        gutenbergTar.extractall(GutenbergDownloads.catalogDir)
        gutenbergTar.close()
        os.remove(tmpFile)

        return True

    def _createCat(self):
        if os.path.isdir(GutenbergDownloads.catalogDir) is False:
            os.mkdir(GutenbergDownloads.catalogDir)
            return True
        return False
