# encoding:utf-8
# name:mdl_config.py
# Author: Aaron Yang
# 2016.07.03

import ConfigParser
import os


def get_config(section, key):

    config = ConfigParser.ConfigParser()
    conf_path = 'db.conf'
    config.read(conf_path)
    return config.get(section, key)
