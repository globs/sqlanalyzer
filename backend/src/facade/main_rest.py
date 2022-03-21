__author__ = "Vincent GAUTIER"
__copyright__ = "Copyright 2022"
__credits__ = ["Vincent GAUTIER"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Vincent GAUTER"
__email__ = "gautier.vincent.ci@gmail.com"
__status__ = "Experimental"
import logging
import traceback
import common.logging
from sqlparser.parser import SQLParser
from flask import Flask, request
import os, json
import psycopg2
from flask_cors import CORS
from werkzeug.security import safe_str_cmp

#Actual flask code
app = Flask(__name__)
cors = CORS(app)
app.debug = True
common.logging.init_logging()


@app.route('/api/v1/logs', methods=['GET', 'POST', 'PUT'])
def logs():
    print('logging')
    logging.info(request.json)


@app.route('/api/v1/parsesql', methods=['GET'])
def parsesql():
    #res = db_get_bots_status()
    parser = SQLParser()
    try:
        parser.parse_sql_script()
    except:
        logging.error(traceback.format_exc())
    return  'test'#json.dumps(res)    


if __name__=="__main__":
	app.run(debug=True, host='0.0.0.0',port='4000')

