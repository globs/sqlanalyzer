import sys
from sqlparser.mysqlparser.sql_lexer import *
import common.logging
import logging


if __name__ == '__main__':
    common.logging.init_logging()
    filename = sys.argv[1]
    file = open(filename)
    characters = file.read()
    file.close()
    logging.info(f"Parsing file content {characters}")
    tokens = sql_lex(characters)
    logging.info(f'Parsing done {tokens}')
    for token in tokens:
        logging.info(token)