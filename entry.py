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
        self.ls_attr_cate = ['A','LF','CF','QF','Q','S','AF','L','C']
        self.ls_doc_type = ['O','S','B']
        self.ls_docinfo_type = ['U','O','S']
        self.ls_info_provided = ['A','T','D','P','O']

        # Modify the content of this list accordingly
        self.ls_attr = ['U','O','S']

    def gen_matrix(self, data, ls_attr):

        pool_kappa = []
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
            result = self.kappa(array, len(data))
            print "Code", kappa_code ,"kappa = ", result[2]
            pool_kappa.append(result)
        pool_p0 = 0
        pool_pe = 0
        pool_divider = 0
        for kappa in pool_kappa:
            pool_p0 += kappa[0]
            pool_pe += kappa[1]
            pool_divider += (1-kappa[1])
        pool_kappa = (pool_p0 - pool_pe) / pool_divider
        print "pool_kappa = ", pool_kappa


    def cal_Kappa(self,data,code_type):

        if code_type is "sources":
            print "Sources:"
            self.gen_matrix(data, self.ls_attr_sources)
        elif code_type is "cate":
            print "Categories:"
            self.gen_matrix(data, self.ls_attr_cate)
        elif code_type is "doctype":
            print "Doctype:"
            #print data
            self.gen_matrix(data, self.ls_doc_type)
        elif code_type is "docinfotype":
            print "Docinfo type:"
            self.gen_matrix(data, self.ls_docinfo_type)
        elif code_type is "infoprovided":
            print "Info provided:"
            self.gen_matrix(data, self.ls_info_provided)
        elif code_type is "common":
            print "calculate general attr:"
            print "code list " , self.ls_attr
            self.gen_matrix(data, self.ls_attr)

    def kappa(self,arr,length):
        p_yes = (arr[0,0]+arr[0,1])/length * (arr[0,0]+arr[1,0])/length
        p_no = (arr[1,0]+arr[1,1])/length * (arr[0,1]+arr[1,1])/length
        p_e = p_yes + p_no
        p0 = (arr[0,0]+arr[1,1]) / length

        kappa = (p0-p_e)/(1-p_e)

        ls = [p0,p_e,kappa]

        return ls



    def entry(self):

        # self.cal_Kappa(self.data_instance.get_sources(), "sources")
        # self.cal_Kappa(self.data_instance.get_cate(), "cate")
        # self.cal_Kappa(self.data_instance.get_doc_type(), "doctype")
        # self.cal_Kappa(self.data_instance.get_docinfo_type(), "docinfotype")
        # self.cal_Kappa(self.data_instance.get_info_provided(),"infoprovided")
        self.cal_Kappa(self.data_instance.get_attr_coder(), "common")




entry = Core()

entry.entry()
