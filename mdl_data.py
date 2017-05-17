# encoding = utf-8
# name = mdl_date.py
# Author: Aaron Yang
# 2016.07.03
import MySQLdb
import mdl_db_manager


class MdlData(object):

    def __init__(self):
        self.db_connection = mdl_db_manager.MdlDatabase()
        self.ls_course_id = []              # store the all the id for the course which contains block learning_analysis
        self.ls_log_tmp = []                # store the tmp log info we need
        self.ls_log = []                    # store the log info
        self.ls_course_elo = []             # store how many elos does a course have, e.g: elo 1,2,3...
        self.ls_elo_component = []          # store the component for each single elo
        self.ls_student_tmp = []            # store student information for every calculation
        self.ls_student = []                # store the calculated result
        self.ls_forum_id = []               # store the forum id result
        self.ls_forum_component_id = []
        self.ls_ouwiki_component_id = []
        self.ls_posts = []

        self.ls_sources = []
        self.ls_cate = []
        self.ls_doctype = []
        self.ls_docinfo_type = []
        self.ls_info_provided = []

        self.ls_common_coder = []

    def is_connected(self):
        return self.db_connection.is_connected()

    def get_sources(self):
        self.ls_sources = self.db_connection.fetch_data("select a.id,a.sources AS sc2,a.coder,b.sources AS sc1,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id "
                                                               "AND a.coder = \"c2\" and b.coder=\"c1\" "
                                                               "AND (a.sources!='' AND b.sources!='') "
                                                               " union "
                                                               "select a.id,a.sources AS sc1,a.coder,b.sources AS sc2,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id AND "
                                                               "a.coder = \"c1\" and b.coder=\"c2\" "
                                                               "AND (a.sources!='' AND b.sources!='')")
        return self.ls_sources

    def get_cate(self):
        self.ls_cate = self.db_connection.fetch_data("select CONCAT(a.cat1,',',a.cat2, ',' ,a.cat3)  AS sc2, "
                                                     "CONCAT(b.cat1,',',b.cat2, ',' ,b.cat3) AS sc1 "
                                                     "from random100 as a, random625 as b "
                                                     "where a.id=b.id AND a.coder = \"c2\" and b.coder=\"c1\" "
                                                     "AND (a.cat1!=\"N/A\" AND a.cat2!=\"N/A\" AND a.cat3!=\"N/A\") "
                                                     "UNION "
                                                     "select CONCAT(a.cat1,',',a.cat2, ',' ,a.cat3)  AS sc1, "
                                                     "CONCAT(b.cat1,',',b.cat2, ',' ,b.cat3) AS sc2 from "
                                                     "random100 as a, random625 as b "
                                                     "where a.id=b.id AND a.coder = \"c1\" and b.coder=\"c2\" "
                                                     "AND (a.cat1!=\"N/A\" AND a.cat2!=\"N/A\" AND a.cat3!=\"N/A\")")
        return self.ls_cate

    def get_doc_type(self):
        self.ls_doctype = self.db_connection.fetch_data("select a.id,a.doctype AS sc2,a.coder,b.doctype AS sc1,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id "
                                                               "AND a.coder = \"c2\" and b.coder=\"c1\" "
                                                               "AND (a.doctype!='' AND b.doctype!='') "
                                                               " union "
                                                               "select a.id,a.doctype AS sc1,a.coder,b.doctype AS sc2,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id AND "
                                                               "a.coder = \"c1\" and b.coder=\"c2\" "
                                                               "AND (a.doctype!='' AND b.doctype!='')")
        return self.ls_doctype

    def get_docinfo_type(self):
        self.ls_docinfo_type = self.db_connection.fetch_data("select a.id,a.docinfotype AS sc2,a.coder,b.docinfotype AS sc1,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id "
                                                               "AND a.coder = \"c2\" and b.coder=\"c1\" "
                                                               "AND (a.docinfotype!='' AND b.docinfotype!='') "
                                                               " union "
                                                               "select a.id,a.docinfotype AS sc1,a.coder,b.docinfotype AS sc2,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id AND "
                                                               "a.coder = \"c1\" and b.coder=\"c2\" "
                                                               "AND (a.docinfotype!='' AND b.docinfotype!='')")
        return self.ls_docinfo_type

    def get_attr_coder(self):
        self.ls_common_coder = self.db_connection.fetch_data("select a.id, a.attr AS sc2, a.coder, b.attr AS sc1, b.coder FROM "
                                                             "random100 as a, random625 as b where a.id=b.id "
                                                             "AND a.coder=\"c2\" and b.coder=\"c1\" "
                                                             "AND (a.attr!='' AND b.attr!='') "
                                                             "union "
                                                             "select a.id, a.attr AS sc1, a.coder, b.attr as sc2, b.coder FROM "
                                                             "random100 as a, random625 as b where a.id=b.id "
                                                             "AND a.coder=\"c1\" and b.coder=\"c2\" "
                                                             "AND (a.attr!='' AND b.attr!='')")
        #print self.ls_common_coder
        return self.ls_common_coder

    def get_info_provided(self):
        self.ls_info_provided = self.db_connection.fetch_data("select a.id,a.infoprovided AS sc2,a.coder,b.infoprovided AS sc1,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id "
                                                               "AND a.coder = \"c2\" and b.coder=\"c1\" "
                                                               "AND (a.infoprovided!='' AND b.infoprovided!='') "
                                                               " union "
                                                               "select a.id,a.infoprovided AS sc1,a.coder,b.infoprovided AS sc2,b.coder FROM "
                                                               "random100 as a, random625 as b where a.id=b.id AND "
                                                               "a.coder = \"c1\" and b.coder=\"c2\" "
                                                               "AND (a.infoprovided!='' AND b.infoprovided!='')")
        return self.ls_info_provided

    def close_connection(self):
        self.db_connection.close_conn()

    # Added 2017.03
    # Functions below are for score prediction




