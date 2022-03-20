
import logging, os, json
from common.dbconnector import dbconnector
from common.file_utils import FileUtils
from common.dbtrace import DbTrace
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

from sqlparser.pysqlparser import select_stmt

class SQLParser:

    def __init__(self):
        logging.debug('Instanciating SQLParser')
        self.dict_results = {}
        self.fileutils = FileUtils()
        self.dbtrace = DbTrace()

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
        #test_sql = self.fileutils.file_to_string('../../samples/sql/DWH/incremental/CR_V_QUOTES.sql')
        test_sql = self.fileutils.file_to_string('../../samples/sql/DWH/incremental/SP_INIT_LOAD_OMEGA_DISCOUNT_CURRENCY.sql')
#        logging.info(test_sql)
#        self.parse_query(test_sql, 1)
#        PrettyJson = json.dumps(self.dict_results, indent=4, separators=(',', ': '), sort_keys=True)
#        logging.info(f"Result: {PrettyJson}")
        for sqlscript in self.fileutils.get_files_from_folder('../../samples/', 'sql'):
            logging.info(f'SQL Script found: {sqlscript}')
            self.dbtrace.trace_to_db(2,1, sqlscript)
            for query in self.fileutils.file_to_string(sqlscript).split(';'):
                if query.upper().find('SELECT') != -1:
                    logging.info(f"parsing {query}")
                    #logging.info(select_stmt.parse_string(query[query.find('SELECT'):len(query)]))
                    self.parse_query(query[query.find('SELECT'):len(query)], 1)
                    PrettyJson = json.dumps(self.dict_results, indent=4, separators=(',', ': '), sort_keys=True)
                    logging.info(f"Result: {PrettyJson}")

    def parse_query(self, sql_query, level):
        # Split a string containing two SQL statements:
        if level > 50:
            return 
        raw = sql_query
        statements = sqlparse.split(raw)

        for statement in statements:
            cleaned_sql = sqlparse.format(statement, strip_comments=True, reindent=True, keyword_case='upper')
            # Parsing a SQL statement:
            logging.info(f"Level {level} Parsing statement {cleaned_sql}")
            parsed = sqlparse.parse(cleaned_sql)[0]
            for token in  parsed.tokens:
                logging.info(f"{type(token)})")
                if  type(token) is sqlparse.sql.Token and str(token).upper().find('SELECT') == -1:
                    if len(str(token)) > 1:
                        logging.info(f"token: {str(token)[0:10]}")
                        if str(token).upper().find('UNION'):
                            self.modif_dict("UNION", 1, 'increment')
                        elif str(token).upper().find('ON'):
                            self.modif_dict("JOIN", 1, 'increment')
                        else:
                            self.modif_dict("TOKEN_TBC", str(token), 'append')
                elif (type(token) is sqlparse.sql.Parenthesis or type(token) is sqlparse.sql.Identifier or type(token) is sqlparse.sql.IdentifierList) and str(token).upper().find('SELECT') != -1:
                    logging.info(f"Subselect detected")
                    self.modif_dict("SUBSELECT", 1, 'increment')
                    sql_parenthesis_stripped = str(token)[1:len(str(token))-2]
                    logging.info(sql_parenthesis_stripped)
                    self.parse_query(sql_parenthesis_stripped, level + 1)
                elif type(token) is sqlparse.sql.Identifier:
                    logging.info(f'Identifier : {token}' )
                    self.modif_dict("IDIENTIFIERS", str(token), 'append')
                elif type(token) is sqlparse.sql.Comparison:
                    logging.info(f'Comparison: {token}' )
                    self.modif_dict("Comparsion", str(token), 'append')
                elif type(token) is sqlparse.sql.Where:
                    logging.info(f'Where condition: {token}' )
                    self.modif_dict("Filter", str(token), 'append')
                elif type(token) is sqlparse.sql.IdentifierList:
                    logging.info(f'Identifier list: {token}' )
                    self.modif_dict("IDIENTIFIER LIST", str(token), 'append')                    
                elif type(token) is sqlparse.sql.Case:
                    logging.info(f'CASE Statement: {token}' )
                    self.modif_dict("CASE", 1, 'increment')    
                elif type(token) is sqlparse.sql.Begin:
                    logging.info(f'Begin Statement: {token}' )
                    self.modif_dict("BEGIN", 1, 'increment')  
                    self.parse_query(str(token), level + 1)    
                else:
                    logging.info(f"Unknow: {str(token)}")
                    self.modif_dict("TYPE_TBC", str(type(token)), 'append')


    def is_subselect(self, parsed):
        if not parsed.is_group:
            return False
        for item in parsed.tokens:
            if item.ttype is DML and item.value.upper() == 'SELECT':
                return True
        return False

    def modif_dict(self, key, value, optype):
        if optype == 'increment':
            if key in self.dict_results:
                self.dict_results[key] = self.dict_results[key] + value
            else: 
                self.dict_results[key] = value
        elif optype == 'append':
            if key in self.dict_results:
                self.dict_results[key].append(value)
            else: 
                self.dict_results[key] = [value]           
