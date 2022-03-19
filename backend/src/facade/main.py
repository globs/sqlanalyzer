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


def main():
    try:
        common.logging.init_logging()
        logging.info("Starting SQL Analyzer")
    except:
        logging.error(traceback.format_exc())



main()