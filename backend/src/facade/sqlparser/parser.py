
import logging, os, json
from common.dbconnector import dbconnector
from common.file_utils import FileUtils
from common.dbtrace import DbTrace
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from concurrent.futures import ThreadPoolExecutor
from sqlparser.pysqlparser import select_stmt
import common.utils 


class SQLParser:

    def __init__(self):
        logging.debug('Instanciating SQLParser')
        self.dict_results = {}
        self.fileutils = FileUtils()
        self.dbtrace = DbTrace()
        self.threadpool = ThreadPoolExecutor(20)
        self.runid = common.utils.gen_uuid()
        logging.info(f"runid: {self.runid}")

    def __del__(self):
        logging.debug('Destructing SQLParser')

    def push_result_to_db(self, filename):
        logging.info(f'Sending json results to db for file {filename}')
        for key in self.dict_results[filename]:
            tmpValue = ""
            data_type =  self.dict_results[filename][key].__class__.__name__
            if data_type == 'list':
                for val in self.dict_results[filename][key]:
                    tmpValue += ',' + val.replace("'", "")
            self.dbtrace.resutls_to_db(filename, self.runid, 1, key, tmpValue, data_type.replace("'", ""))


    @dbconnector
    def parse_sql_script(cnn, self):
        logging.info('Parsing SQL Queries')
#        sql = "select * from api_data.tgen_code_value"
#        cursor = cnn.cursor()
#        cursor.execute(sql)       
#        codes = cursor.fetchall()
#        for code in codes:
#            logging.info(f"code id: {code[0]}, ts:{code[1]}")
#        logging.info(os.getcwd())
        #test_sql = self.fileutils.file_to_string('../../samples/sql/DWH/incremental/CR_V_QUOTES.sql')
#        test_sql = self.fileutils.file_to_string('../../samples/sql/DWH/incremental/SP_INIT_LOAD_OMEGA_DISCOUNT_CURRENCY.sql')
#        logging.info(test_sql)
#        self.parse_query(test_sql, 1)
#        PrettyJson = json.dumps(self.dict_results, indent=4, separators=(',', ': '), sort_keys=True)
#        logging.info(f"Result: {PrettyJson}")
        for sqlscript in self.fileutils.get_files_from_folder('../../samples/', 'sql'):
            logging.info(f'SQL Script found: {sqlscript}')
            self.dict_results[sqlscript] = {}
            self.threadpool.submit(self.parse_queries, sqlscript)
        logging.info('All work submitted, waiting for shutdown ...')
        self.threadpool.shutdown(wait=True)
        logging.info('Shutting down')



    def parse_queries(self, sqlscript):
        self.dbtrace.trace_to_db(self.runid, 2, 1, sqlscript)
        self.dbtrace.trace_to_db(self.runid, 2, 3, str(self.fileutils.file_size(sqlscript)) + '-' + sqlscript)
        for query in self.fileutils.file_to_string(sqlscript).split(';'):
            if query.upper().find('SELECT') != -1:
                logging.debug(f"parsing {query}")
                #logging.info(select_stmt.parse_string(query[query.find('SELECT'):len(query)]))
                self.parse_query(sqlscript ,query[query.find('SELECT'):len(query)], 1)
                PrettyJson = json.dumps(self.dict_results, indent=4, separators=(',', ': '), sort_keys=True)
                logging.debug(f"Result: {PrettyJson}")
        logging.info(f"Parsing ended ok for: {sqlscript}")
        self.push_result_to_db(sqlscript)
        self.dbtrace.trace_to_db(self.runid, 2, 2, sqlscript)

    def parse_query(self, filename, sql_query, level):
        # Split a string containing two SQL statements:
        if level > 50:
            return 
        raw = sql_query
        statements = sqlparse.split(raw)

        for statement in statements:
            cleaned_sql = sqlparse.format(statement, strip_comments=True, reindent=True, keyword_case='upper')
            # Parsing a SQL statement:
            logging.debug(f"Level {level} Parsing statement {cleaned_sql}")
            parsed = sqlparse.parse(cleaned_sql)[0]
            for token in  parsed.tokens:
                logging.debug(f"{type(token)})")
                if  type(token) is sqlparse.sql.Token and str(token).upper().find('SELECT') == -1:
                    if len(str(token)) > 1:
                        logging.debug(f"token: {str(token)[0:10]}")
                        if str(token).upper().find('UNION'):
                            self.modif_dict(filename, "UNION", 1, 'increment')
                        elif str(token).upper().find('ON'):
                            self.modif_dict(filename, "JOIN", 1, 'increment')
                        else:
                            self.modif_dict(filename, "TOKEN_TBC", str(token), 'append')
                elif (type(token) is sqlparse.sql.Parenthesis or type(token) is sqlparse.sql.Identifier or type(token) is sqlparse.sql.IdentifierList) and str(token).upper().find('SELECT') != -1:
                    logging.debug(f"Subselect detected")
                    self.modif_dict(filename, "SUBSELECT", 1, 'increment')
                    sql_parenthesis_stripped = str(token)[1:len(str(token))-2]
                    logging.debug(sql_parenthesis_stripped)
                    self.parse_query(sql_parenthesis_stripped, level + 1)
                elif type(token) is sqlparse.sql.Identifier:
                    logging.debug(f'Identifier : {token}' )
                    self.modif_dict(filename, "IDIENTIFIERS", str(token), 'append')
                elif type(token) is sqlparse.sql.Comparison:
                    logging.debug(f'Comparison: {token}' )
                    self.modif_dict(filename, "Comparsion", str(token), 'append')
                elif type(token) is sqlparse.sql.Where:
                    logging.debug(f'Where condition: {token}' )
                    self.modif_dict(filename, "Filter", str(token), 'append')
                elif type(token) is sqlparse.sql.IdentifierList:
                    logging.debug(f'Identifier list: {token}' )
                    self.modif_dict(filename, "IDIENTIFIER LIST", str(token), 'append')                    
                elif type(token) is sqlparse.sql.Case:
                    logging.debug(f'CASE Statement: {token}' )
                    self.modif_dict(filename, "CASE", 1, 'increment')    
                elif type(token) is sqlparse.sql.Begin:
                    logging.debug(f'Begin Statement: {token}' )
                    self.modif_dict(filename, "BEGIN", 1, 'increment')  
                    self.parse_query(str(token), level + 1)    
                else:
                    logging.debug(f"Unknow: {str(token)}")
                    #self.modif_dict(filename, "TYPE_TBC", token.__class__.__name__, 'append')


    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

    def modif_dict(self, file, key, value, optype):
        if optype == 'increment':
            if key in self.dict_results:
                self.dict_results[file][key] = self.dict_results[key] + value
            else: 
                self.dict_results[file][key] = value
        elif optype == 'append':
            if key in self.dict_results:
                self.dict_results[file][key].append(value)
            else: 
                self.dict_results[file][key] = [value]           
