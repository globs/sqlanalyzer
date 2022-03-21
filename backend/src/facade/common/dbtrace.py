import psycopg2
import logging
import common.settings
from common.dbconnector import dbconnector


class DbTrace:

    def __init__(self):
        logging.debug('Instanciating  dbtrace')

    def __del__(self):
        logging.debug('Destructing dbtrace')
    
    @dbconnector
    def trace_to_db(cnn, self, uuid, message_level, message_type, message):
        sql = f"""
        INSERT INTO api_data.trun_traces  
        (UUID, MESSAGE_LEVEL, MESSAGE_TYPE_ID, MESSAGE_TEXT)
        VALUES
        ('{uuid}', {message_level}, {message_type}, '{message}');

        """
        logging.debug(f'Trace Query: {sql}')
        cursor = cnn.cursor()
        cursor.execute(sql)    
        cnn.commit()
        cursor.close()   

    @dbconnector
    def resutls_to_db(cnn, self, filename, uuid, analyzer_id, key, value, data_type):
        sql = f"""
        INSERT INTO api_data.tsql_analysis_traces  
        (FILENAME, UUID,ANALYZER_ID,ANALYZER_KEY,ANALYZER_VALUE,VALUE_TYPE)
        VALUES
        ('{filename}', '{uuid}', {analyzer_id}, '{key}', '{value}', '{data_type}');

        """
        logging.debug(f'Trace Query: {sql}')
        cursor = cnn.cursor()
        cursor.execute(sql)    
        cnn.commit()
        cursor.close()   
