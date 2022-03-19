
import logging
from common.dbconnector import dbconnector

class SQLParser:

    def __init__(self):
        logging.debug('Instanciating SQLParser')
        self.dict_results = {}

    def __del__(self):
        logging.debug('Destructing Binance SQLParser')

    @dbconnector
    def parse_sql_query(self):
        logging.info('Parsing SQL Query')
        