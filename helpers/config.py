import configparser
import logging
import os
import sys

class GutenbergConfig:
    def __init__(self):
        self.config_path = "gutenberg.conf"
        self.logger = logging.getLogger('guten_logs')
        self._verifyConfig()

        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)


    def _verifyConfig(self):
        self.logger.debug("Verifying existence of configuration file")
        if os.path.isfile(self.config_path) is False:
            self.logger.error("CONFIGURATION FILE MISSING. EXIT")
            sys.exit(1)
        return True

    def getConfigValue(self, section, field):
        return self.config[section][field]

    def getConfigSection(self, section):
        return self.config[section]
