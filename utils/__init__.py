# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import importlib
import json
import logging
import os
import pkgutil
import pprint
from logging.handlers import RotatingFileHandler

import pandas as pd
import pkg_resources
from pkg_resources import get_distribution, DistributionNotFound

from consts import AKMYSQL_HOME

logger = logging.getLogger(__name__)

akmysql_env = {}
akmysql_config = {}

def init_log(file_name='akmysql.log', log_dir=None, simple_formatter=True):
    if not log_dir:
        log_dir = akmysql_env['log_path']

    root_logger = logging.getLogger()

    # reset the handlers
    root_logger.handlers = []

    root_logger.setLevel(logging.INFO)

    file_name = os.path.join(log_dir, file_name)

    file_log_handler = RotatingFileHandler(file_name, maxBytes=524288000, backupCount=10)

    file_log_handler.setLevel(logging.INFO)

    console_log_handler = logging.StreamHandler()
    console_log_handler.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    if simple_formatter:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s  %(threadName)s  %(message)s")
    else:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s  %(threadName)s  %(name)s:%(filename)s:%(lineno)s  %(funcName)s  %(message)s")
    file_log_handler.setFormatter(formatter)
    console_log_handler.setFormatter(formatter)

    # add the handlers to the logger
    root_logger.addHandler(file_log_handler)
    root_logger.addHandler(console_log_handler)


def init_env(AKMYSQL_HOME: str, **kwargs) -> dict:
    akmysql_env['AKMYSQL_HOME'] = AKMYSQL_HOME

    # path for storing logs
    akmysql_env['log_path'] = os.path.join(AKMYSQL_HOME, 'logs')
    if not os.path.exists(akmysql_env['log_path']):
        os.makedirs(akmysql_env['log_path'])
    akmysql_env['csv_path'] = os.path.join(AKMYSQL_HOME, 'csvs')
    if not os.path.exists(akmysql_env['csv_path']):
        os.makedirs(akmysql_env['csv_path'])
    akmysql_env['jpg_path'] = os.path.join(AKMYSQL_HOME, 'jpgs')
    if not os.path.exists(akmysql_env['jpg_path']):
        os.makedirs(akmysql_env['jpg_path'])
    # init config
    init_config('akmysql',current_config=akmysql_config, **kwargs)
    pprint.pprint(akmysql_env)
    return akmysql_env

def init_config(pkg_name: str = None,current_config: dict = None, **kwargs) -> dict:
    # create default config.json if not exist
    if pkg_name:
        config_file = f'{pkg_name}_config.json'
    else:
        pkg_name = 'akmysql'
        config_file = 'config.json'

    logger.info(f'init config for {pkg_name}, current_config:{current_config}')

    config_path = os.path.join(akmysql_env['AKMYSQL_HOME'], config_file)
    if not os.path.exists(config_path):
        from shutil import copyfile
        try:
            sample_config = pkg_resources.resource_filename(pkg_name, 'config.json')
            if os.path.exists(sample_config):
                copyfile(sample_config, config_path)
        except Exception as e:
            logger.warning(f'could not load config.json from package {pkg_name}')

    if os.path.exists(config_path):
        with open(config_path) as f:
            config_json = json.load(f)
            for k in config_json:
                current_config[k] = config_json[k]

    # set and save the config
    for k in kwargs:
        current_config[k] = kwargs[k]
        with open(config_path, 'w+') as outfile:
            json.dump(current_config, outfile)

    pprint.pprint(current_config)
    return current_config


init_env(AKMYSQL_HOME=AKMYSQL_HOME)

# the __all__ is generated
#__all__ = []
__all__ = ['akmysql_env', 'akmysql_config']

