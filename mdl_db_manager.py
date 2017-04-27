# encoding = utf-8
# name:mdl_db_manager.py
# Author: Aaron Yang
# 2016.07.04

import MySQLdb
import MySQLdb.cursors
import mdl_config


DB = "database"
DB_HOST = mdl_config.get_config(DB, 'db_host')
DB_PORT = mdl_config.get_config(DB, "db_port")
DB_NAME = mdl_config.get_config(DB, 'db_name')
DB_USER = mdl_config.get_config(DB, 'db_user')
DB_PWD = mdl_config.get_config(DB, 'db_password')
DB_CHARSET = mdl_config.get_config(DB, 'db_charset')


class MdlDatabase(object):

    def __init__(self, db_name_input=None, db_host_input=None):
        if db_name_input is None:
            self.db_name = DB_NAME
        else:
            self.db_name = db_name_input
        if db_host_input is None:
            self.db_host = DB_HOST
        else:
            self.db_host = db_host_input

        self.is_conn = False
        self.db_user = DB_USER
        self.db_password = DB_PWD
        self.db_charset = DB_CHARSET
        self.db_port = int(DB_PORT)
        self.conn = None

        if self.connect_mysql():
            self.db_cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def connect_mysql(self):
        try:
            self.conn = MySQLdb.connect(host=self.db_host,
                                   port=self.db_port,
                                   db=self.db_name,
                                   user=self.db_user,
                                   passwd=self.db_password,
                                   charset=self.db_charset
                                   )
            self.is_conn = True
            return True
        except Exception, data:
            self.is_conn = False
            print "connection fail: " + data.__str__()
            return False

    def is_connected(self):
        if self.is_conn:
            return self.conn
        else:
            return False

    def fetch_data(self, sql, argument=[]):
        array_result = []
        if self.is_conn:
            try:
                if type(argument) is list:
                    if argument is []:
                        self.db_cursor.execute(sql)
                    else:
                        self.db_cursor.execute(sql, argument)
                else:
                    self.db_cursor.execute(sql, [argument])
                row_count = self.db_cursor.rowcount
                for i in range(0, row_count):
                    row = self.db_cursor.fetchone()
                    array_result.append(row)
            except Exception, data:
                return "fetch data fail with " + sql + data.__str__(), "argument = ", argument
        return array_result

    def insert_data(self, sql, params):
        success = False
        #print sql, params
        if self.is_conn:
            try:
                self.db_cursor.executemany(sql, params)
                self.conn.commit()
                success = True
                return success
            except Exception, data:
                success = False
                return "insert database fail with " + sql + data.__str__()

    def update_data(self, sql, params=[]):
        success = False
        if self.is_conn:
            try:
                self.db_cursor.execute(sql, params)
                self.conn.commit()
                success = True
            except Exception, data:
                success = False
                return "update database fail with " + sql + data.__str__()
        return success

    def clear_data(self, sql):
        success = False
        if self.is_conn:
            try:
                self.db_cursor.execute(sql)
                success = True
            except Exception, data:
                success = False
                return "Clear fail with" + sql + data.__str__()
        return success

    def close_conn(self):
        if self.is_conn:
            try:
                if type(self.db_cursor) == 'object':
                    self.db_cursor.close()
                if type(self.conn) == 'object':
                    self.conn.close()
            except Exception, data:
                return "close db error: " + data.__str__()





