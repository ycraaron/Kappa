from __future__ import division
# from meliae import scanner
# from meliae import loader

import mdl_logger
import math
import mdl_data
import logging
import logging.handlers
import numpy


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

        self.ls_attr_sources = ['O', 'W', 'L']
        self.ls_attr_cate = ['A','LF','CF','N/A','QF','Q','S','AF','L','C']

    def gen_matrix(self, data, ls_attr):

        for kappa_code in ls_attr:
            array = numpy.zeros((2, 2))
            for item in data:
                in_tag_sc1 = 0
                if ',' in item["sc1"]:
                    arr_sp = item["sc1"].split(',')
                    for code in arr_sp:
                        if code == kappa_code:
                            in_tag_sc1 = 1

                in_tag_sc2 = 0
                if ',' in item["sc2"]:
                    arr_sp = item["sc2"].split(',')
                    for code in arr_sp:
                        if code == kappa_code:
                            in_tag_sc2 = 1

                if in_tag_sc1 == 1:
                    if in_tag_sc2 == 1:
                        array[0, 0] += 1
                    elif ',' in item["sc2"]:
                        array[0, 1] += 1
                    elif ',' not in item["sc2"]:
                        if item["sc2"] == kappa_code:
                            array[0, 0] += 1
                        else:
                            array[0, 1] += 1
                else:
                    if ',' in item["sc1"]:
                        if in_tag_sc2 == 1:
                            array[1, 0] += 1
                        elif ',' in item["sc2"]:
                            array[1, 1] += 1
                        elif ',' not in item["sc2"]:
                            if item["sc2"] == kappa_code:
                                array[1, 0] += 1
                            else:
                                array[1, 1] += 1
                    else:
                        if item["sc1"] == kappa_code:
                            if in_tag_sc2 == 1:
                                array[0, 0] += 1
                            elif ',' in item["sc2"]:
                                array[0, 1] += 1
                            elif ',' not in item["sc2"]:
                                if item["sc2"] == kappa_code:
                                    array[0, 0] += 1
                                else:
                                    array[0, 1] += 1
                        else:
                            if in_tag_sc2 == 1:
                                array[1, 0] += 1
                            elif ',' in item["sc2"]:
                                array[1, 1] += 1
                            elif ',' not in item["sc2"]:
                                if item["sc2"] == kappa_code:
                                    array[1, 0] += 1
                                else:
                                    array[1, 1] += 1

            print array
            print self.kappa(array, len(data))


            #
            # if item["sc1"] == kappa_code:
            #     if item["sc2"] == kappa_code:
            #         array[0,0] += 1
            #     else:
            #         array[0,1] += 1
            # else:
            #     if item["sc2"] == kappa_code:
            #         array[1,0] += 1
            #     else:
            #         array[1,1] += 1


            #
            # if kappa_code in item["sc1"]:
            #     if kappa_code in item["sc2"]:
            #         array[0, 0] += 1
            #     else:
            #         array[0, 1] += 1
            # else:
            #     if kappa_code in item["sc2"]:
            #         array[1, 0] += 1
            #     else:
            #         array[1, 1] += 1

    def cal_Kappa(self,data,code_type):

        if code_type is "sources":
            self.gen_matrix(data, self.ls_attr_sources)
        elif code_type is "cate":
            self.gen_matrix(data, self.ls_attr_cate)


    def kappa(self,arr,length):
        p_yes = (arr[0,0]+arr[0,1])/length * (arr[0,0]+arr[1,0])/length
        p_no = (arr[1,0]+arr[1,1])/length * (arr[0,1]+arr[1,1])/length
        p_e = p_yes + p_no
        p0 = (arr[0,0]+arr[1,1]) / length

        kappa = (p0-p_e)/(1-p_e)

        return kappa



    def entry(self):
        #print self.data_instance.get_sources()

        self.cal_Kappa(self.data_instance.get_sources(),"sources")



entry = Core()

entry.entry()
