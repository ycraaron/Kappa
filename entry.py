from __future__ import division
# from meliae import scanner
# from meliae import loader

import mdl_logger
import math
import mdl_data
import logging
import logging.handlers


class Core:

    def __init__(self):
        LOG_FILE = 'kappa.log'
        self.handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=5)
        self.fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'
        self.consoleHandler = logging.StreamHandler()
        self.formatter = logging.Formatter(self.fmt)
        self.handler.setFormatter(self.formatter)
        self.consoleHandler.setFormatter(self.formatter)
        self.logger = logging.getLogger('mdl_logger')
        self.logger.addHandler(self.handler)
        self.logger.addHandler(self.consoleHandler)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("New transaction starts")

        self.data_instance = mdl_data.MdlData()

        print self.data_instance.get_sources()


    def cal_Kappa
entry = Core()
