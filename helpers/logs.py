import logging

class GutenbergLogs:

    levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    logPath = "logs/gutenberg.log"
    logFormat = '%(asctime)s | %(name)s_%(levelname)s: %(message)s'

    def __init__(self):
        self.logger = logging.getLogger("guten_logs")
        self.level = 'warning'
        self.setLevel(self.level)

        self.logFile = self.setLogFile(GutenbergLogs.logPath)

        self.setFormat(GutenbergLogs.logFormat)

    def setLevel(self, level):
        loggingLevel = GutenbergLogs.levels[level]
        self.logger.setLevel(loggingLevel)
        self.level = level

    def setLogFile(self, file):
        logFile = logging.FileHandler(file)
        self.logger.addHandler(logFile)
        return logFile

    def setFormat(self, format):
        formatter = logging.Formatter(format)
        self.logFile.setFormatter(formatter)
