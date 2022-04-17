
import logging, os, json, traceback
from common.dbconnector import dbconnector
from common.file_utils import FileUtils
from common.dbtrace import DbTrace
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from concurrent.futures import ThreadPoolExecutor
from sqlparser.pysqlparser import select_stmt
import common.utils 
import io
import pandas as pd

class SQLParser:

    def __init__(self):
        logging.debug('Instanciating SQLParser')
        self.dict_results = {}
        self.fileutils = FileUtils()
        self.dbtrace = DbTrace()
        self.threadpool = ThreadPoolExecutor(20)
        self.runid = common.utils.gen_uuid()
        logging.info(f"runid: {self.runid}")
        self.internal_delim = '~'

    def __del__(self):
        logging.debug('Destructing SQLParser')

    def clean_csv_value(self, value):
        res = str(value)
        if value is None:
            res = r'\N'
        else:
            if "'" in res:
                res = res.replace("'", "''" )
            elif "~" in res:
                res = res.replace("~", "")
        return res.replace('\n', '\\n')  

    @dbconnector
    def push_result_to_db(cnn, self, filename):
        symbols_stringio = io.StringIO()
        logging.info(f'Sending json results to db for file {filename}')
        cursor = cnn.cursor()
        with open(filename, 'r') as filehander:
            content = filehander.read()
            freqs = self.fileutils.word_freq(content)
            for freq in freqs:
                #logging.info(f"{freq}")
                symbols_stringio.write(self.internal_delim.join(map(self.clean_csv_value, (filename, 'all', self.runid, 2, freq['word'].replace("'","''"), freq['freq'], 'int'))) + '\n')            
        symbols_stringio.seek(0)
        logging.info(f"{symbols_stringio.getvalue()}")
        cursor.copy_from(symbols_stringio, 'tsql_analysis_traces', sep=self.internal_delim, columns=('filename' ,'query', 'uuid', 'analyzer_id', 'analyzer_key', 'analyzer_value', 'value_type'))
        cnn.commit()
        cursor.close()   
        cnn.close()
        for query_key in self.dict_results[filename]:
            for key in self.dict_results[filename][query_key]:
                tmpValue = ""
                data_type =  self.dict_results[filename][query_key][key].__class__.__name__
                if data_type == 'list':
                    for val in self.dict_results[filename][query_key][key]:
                        tmpValue += ', ' + val.replace("'", "")
                else:
                    tmpValue = self.dict_results[filename][query_key][key]
                self.dbtrace.resutls_to_db(filename, query_key, self.runid, 1, key, tmpValue, data_type.replace("'", ""))


    @dbconnector
    def parse_sql_script(cnn, self):
        try:
            logging.info('Parsing SQL Queries')
    #        sql = "select * from api_data.tgen_code_value"
    #        cursor = cnn.cursor()
    #        cursor.execute(sql)       
    #        codes = cursor.fetchall()
    #        for code in codes:
    #            logging.info(f"code id: {code[0]}, ts:{code[1]}")
    #        logging.info(os.getcwd())
            #test_sql = self.fileutils.file_to_string('../../samples/sql/DWH/incremental/CR_V_QUOTES.sql')
    #     test_sql = self.fileutils.file_to_string()
    #     logging.info(test_sql)
            #self.dict_results['../../samples/sql/DWH/incremental/V_CONTROL_STATS_I17_ATTRIBUTE_LOCAL.sql'] = {}
            #self.parse_queries('../../samples/sql/DWH/incremental/V_CONTROL_STATS_I17_ATTRIBUTE_LOCAL.sql')
    #        PrettyJson = json.dumps(self.dict_results, indent=4, separators=(',', ': '), sort_keys=True)
    #        logging.info(f"Result: {PrettyJson}")
            for sqlscript in self.fileutils.get_files_from_folder('../../samples/', 'sql'):
                logging.info(f'SQL Script found: {sqlscript}')
                self.dict_results[sqlscript] = {}
                #self.parse_queries(sqlscript)
                self.threadpool.submit(self.parse_queries, sqlscript)
            logging.info('All work submitted, waiting for shutdown ...')
            self.threadpool.shutdown(wait=True)
            logging.info('Shutting down')
            PrettyJson = json.dumps(self.dict_results, indent=4, separators=(',', ': '), sort_keys=True)
            #logging.info(f"Result: {PrettyJson}")
        except:
            logging.error(traceback.format_exc())            

    def parse_queries(self, sqlscript):
        logging.debug('SQL Script: ')
        i = 0
        try:
            self.dbtrace.trace_to_db(self.runid, 2, 1, sqlscript)
            self.dbtrace.trace_to_db(self.runid, 2, 3, str(self.fileutils.file_size(sqlscript)) + '-' + sqlscript)
            for query in self.fileutils.file_to_string(sqlscript).split(';'):
                i = i + 1
                query_key= f'query{i}'

                self.dict_results[sqlscript][query_key] = {}
                if query.upper().find('WITH DATA') != -1:
                    self.modif_dict(sqlscript, query_key, "BLOCKING WITH DATA", 1, 'increment')
                if query.upper().find('DISTINCT') != -1:
                    self.modif_dict(sqlscript, query_key, "DISTINCT", 1, 'increment')
                else:
                    logging.debug(f'distinct not found in {query}')
                if query.upper().find('SELECT') != -1:
                    logging.debug(f"parsing {query[0:10]} ...")
                    #logging.debug(f"cut: {query[query.find('SELECT'):len(query)]}")
                    #logging.info(query[query.find('SELECT'):len(query)])
                    self.parse_query(sqlscript , query_key, query[query.find('SELECT'):len(query)], 1)
            logging.info('pushing result to db for ')
            self.push_result_to_db(sqlscript)
            logging.info(f"Parsing ended ok for: {sqlscript}")
            self.dbtrace.trace_to_db(self.runid, 2, 2, sqlscript)
        except:
            logging.info(traceback.format_exc())            

    def parse_query(self, filename, query_key, sql_query, level):
        # Split a string containing two SQL statements:
        if level > 50:
            return 
        raw = sql_query
        statements = sqlparse.split(raw)

        for statement in statements:
            #cleaned_sql = sqlparse.format(statement, strip_comments=True, reindent=True, keyword_case='upper')
            # Parsing a SQL statement:
            logging.info(f"Level {level} ")
            logging.debug(f"Parsing statement {statement}")
            parsed = sqlparse.parse(statement)[0]
            for token in  parsed.tokens:
                logging.debug(f"{type(token)})")
                if  type(token) is sqlparse.sql.Token and str(token).upper().find('SELECT') == -1:
                    if len(str(token)) > 1:
                        logging.debug(f"token: {str(token)[0:10]}")
                        if str(token).upper().find('UNION'):
                            self.modif_dict(filename, query_key, "UNION", 1, 'increment')
                        elif str(token).upper().find('ON'):
                            self.modif_dict(filename, query_key, "JOIN", 1, 'increment')
                        else:
                            self.modif_dict(filename, query_key, "TOKEN_TBC", str(token), 'append')
                elif (type(token) is sqlparse.sql.Parenthesis or type(token) is sqlparse.sql.Identifier or type(token) is sqlparse.sql.IdentifierList) and str(token).upper().find('SELECT') != -1:
                    logging.debug(f"Subselect detected")
                    self.modif_dict(filename, query_key, "SUBSELECT", 1, 'increment')
                    sql_parenthesis_stripped = str(token)[1:len(str(token))-2]
                    logging.debug(sql_parenthesis_stripped)
                    self.parse_query(filename, query_key, sql_parenthesis_stripped, level + 1)
                elif type(token) is sqlparse.sql.Identifier:
                    logging.debug(f'Identifier : {token}' )
                    self.modif_dict(filename, query_key, "IDIENTIFIERS", str(token), 'append')
                elif type(token) is sqlparse.sql.Comparison:
                    logging.debug(f'Comparison: {token}' )
                    self.modif_dict(filename, query_key, "Comparsion", str(token), 'append')
                elif type(token) is sqlparse.sql.Where:
                    logging.debug(f'Where condition: {token}' )
                    self.modif_dict(filename, query_key, "Filter", str(token), 'append')
                elif type(token) is sqlparse.sql.IdentifierList:
                    logging.debug(f'Identifier list: {token}' )
                    self.modif_dict(filename, query_key, "IDIENTIFIER LIST", str(token), 'append')                    
                elif type(token) is sqlparse.sql.Case:
                    logging.debug(f'CASE Statement: {token}' )
                    self.modif_dict(filename, query_key, "CASE", 1, 'increment')    
                elif type(token) is sqlparse.sql.Begin:
                    logging.debug(f'Begin Statement: {token}' )
                    self.modif_dict(filename, query_key, "BEGIN", 1, 'increment')  
                    self.parse_query(str(token), level + 1)    
                else:
                    logging.debug(f"Unknow: {str(token)}")
                    self.modif_dict(filename, query_key, "  ", token.__class__.__name__.replace("'",""), 'append')


    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

    def modif_dict(self, file, query_key, key, value, optype):
        if optype == 'increment':
            if key in self.dict_results[file][query_key]:
                self.dict_results[file][query_key][key] = self.dict_results[file][query_key][key] + value
            else: 
                self.dict_results[file][query_key][key] = value
        elif optype == 'append':
            if key in self.dict_results[file][query_key]:
                self.dict_results[file][query_key][key].append(value)
            else: 
                self.dict_results[file][query_key][key] = [value]           
        #logging.info(f"Key changed {self.dict_results[file][query_key][key]}")

