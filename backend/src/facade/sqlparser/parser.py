
import logging, os
from common.dbconnector import dbconnector
from common.file_utils import FileUtils
import sqlparse

class SQLParser:

    def __init__(self):
        logging.debug('Instanciating SQLParser')
        self.dict_results = {}
        self.fileutils = FileUtils()

    def __del__(self):
        logging.debug('Destructing SQLParser')

    @dbconnector
    def parse_sql_script(cnn, self):
        logging.info('Parsing SQL Query')
        sql = "select * from api_data.tgen_code_value"
        cursor = cnn.cursor()
        cursor.execute(sql)       
        codes = cursor.fetchall()
        for code in codes:
            logging.info(f"code id: {code[0]}, ts:{code[1]}")
        logging.info(os.getcwd())
        test_sql = self.fileutils.file_to_string('../../samples/sql/DWH/incremental/CR_V_QUOTES.sql')
        logging.info(test_sql)
        self.parse_query(test_sql)

    def parse_query(self, sql_query):
        # Split a string containing two SQL statements:
        raw = sql_query
        statements = sqlparse.split(raw)

        for statement in statements:
            #logging.info(sqlparse.format(statement, reindent=True, keyword_case='upper'))
            # Parsing a SQL statement:
            logging.info('Parsing statement')
            parsed = sqlparse.parse(statement)[0]
            for token in  parsed.tokens:
                logging.info(f"{type(token)})")