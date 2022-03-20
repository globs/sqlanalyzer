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
    def trace_to_db(cnn, self, message_level, message_type, message):
        logging.info('Parsing SQL Query')
        sql = f"""
        INSERT INTO api_data.trun_traces  
        (MESSAGE_LEVEL, MESSAGE_TYPE_ID, MESSAGE_TEXT)
        VALUES
        ({message_level}, {message_type}, '{message}');

        """
        cursor = cnn.cursor()
        cursor.execute(sql)    
        cnn.commit()
        cursor.close()   