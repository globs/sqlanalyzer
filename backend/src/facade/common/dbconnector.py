import psycopg2
import logging
import common.settings


def dbconnector(func):
    def with_connection_(*args,**kwargs):
        cnn = psycopg2.connect(user = common.settings.pg_user,
                                  password = common.settings.pg_password,
                                  host = common.settings.pg_host,
                                  port = common.settings.pg_port,
                                  database = common.settings.pg_app_db,
                                  options="-c search_path=dbo,public,api_data")
                                  
        cnn.autocommit=True
        try:
            rv = func(cnn, *args,**kwargs)
        except Exception as e:
            cnn.rollback()
            logging.error(f'Database connection error: {e}')
            raise
        else:
            logging.debug('query executed successfully')
        finally:
            cnn.close()
        return rv
    return with_connection_