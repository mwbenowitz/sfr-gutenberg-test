import requests
import os
import shutil
import sys
import logging
import time
import tarfile

class GutenbergDownloads:
    def __init__(self):
        self.logger = logging.getLogger("guten_logs")
        self.file_dir = "files/"
        self.catalog_dir = self.file_dir + "gutenberg_catalog"

        self.gutenberg_catalog = "https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2"

        self.run_time = time.time()
        self.cutoff_time = self.run_time - 86400

    def get_rdf_records(self):
        self.logger.debug("Checking time of last RDF Catalog Download")
        # Check if the path exists and if this was run in the past 24 hours
        catalog_modified = os.path.getmtime(self.catalog_dir)
        if (
            os.path.isdir(self.catalog_dir) is False or
            self.cutoff_time > catalog_modified
        ):
            self.logger.info("RDF Catalog is expired, downloading a fresh copy")
            # If not, get the tar from Gutenberg
            self.logger.info("Deleting existing copy")
            shutil.rmtree(self.catalog_dir)
            gutenberg_zip = requests.get(self.gutenberg_catalog)
            if gutenberg_zip.status_code == 200:
                self.logger.debug("Unziping RDF tar.bz2...")
                tmp_file = self.file_dir + "gutenberg-rdf.tar.bz2"
                opn_file = open(tmp_file, "wb")
                opn_file.write(gutenberg_zip.content)
                opn_file.close()
                gutenberg_tar = tarfile.open(tmp_file, "r:bz2")
                self.logger.debug("Extracting to a directory...")
                gutenberg_tar.extractall(self.catalog_dir)
                gutenberg_tar.close()
                os.remove(tmp_file)
            else:
                self.logger.error("COULD NOT DOWNLOAD RDF FILE! EXITING")
                sys.exit(2)
        else:
            self.logger.debug("RDF Catalog is current, no action is necessary")
