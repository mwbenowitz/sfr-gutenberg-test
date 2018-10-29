import argparse
import logging

LOGGER = logging.getLogger(__name__)

class GutenbergArgs:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="""
            Process the parameters for how to process Gutenberg records
        """)

        self.parser.add_argument('-f', '--full', action='store_true', help="Run a full import")
        self.parser.add_argument('--DROPDB', action='store_true', help="Drop and recreate the database")
        self.parser.add_argument('-l', '--level', help="Set the log level")
