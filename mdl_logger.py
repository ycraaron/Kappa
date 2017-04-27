import logging
import logging.handlers

LOG_FILE = "kappa.log"

class MdlLogger(object):

    def __init__(self):
        self.handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)
        self.fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        self.formatter = logging.Formatter(self.fmt)
        self.handler.setFormatter(self.formatter)
        self.logger = logging.getLogger('mdl_logger')
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)

    def logInfo(self, logtxt):
        self.logger.info(logtxt)
        print logtxt

    def logDebug(self, logtxt):
        self.logger.debug(logtxt)
