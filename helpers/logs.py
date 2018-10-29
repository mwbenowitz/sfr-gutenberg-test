import logging

class GutenbergLogs:

    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    log_file_path = "logs/gutenberg.log"
    log_format = '%(asctime)s | %(name)s_%(levelname)s: %(message)s'

    def __init__(self):
        self.logger = logging.getLogger("guten_logs")
        self.level = 'warning'
        self.setLevel(self.level)

        self.log_file = self.setLogFile(log_file_path)

        self.setFormat(log_format)

    def setLevel(self, level):
        logging_level = levels[level]
        self.logger.setLevel(logging_level)
        self.level = level

    def setLogFile(self, file):
        log_file = logging.FileHandler(file)
        self.logger.addHandler(log_file)
        return log_file

    def setFormat(self, format):
        formatter = logging.Formatter(format)
        self.log_file.setFormatter(formatter)
