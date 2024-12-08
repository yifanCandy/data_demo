import logging
import os
import oracledb
import json
# import data_upload.cache as cache_data
import time
from datetime import datetime
# from .file_util import FileUtil

class Data_logic:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('data_logic')
        self.logger.info("init class data_logic.")

    def demo_test(self, config):
        database_host = config["database"]["host"]
        self.logger.info(f"database_host: {database_host}")
        




    
        







    