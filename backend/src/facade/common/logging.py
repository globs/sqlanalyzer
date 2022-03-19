import logging, decimal
from logging.config import dictConfig

app_log_level=logging.INFO

def init_logging():
	dictconfig = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - {%(filename)s:%(lineno)d}: %(levelname)s - %(message)s"
        }
    },
 
    "handlers": {
        "file_handler": {
            "class": "logging.FileHandler",
            "level": app_log_level,
            "formatter": "simple",
            "filename": "../logs/sql_analyzer.log",
            "encoding": "utf8"},
        "console_handler": {
			'class': 'logging.StreamHandler',
              'formatter': 'simple',
              'level': app_log_level}
    },
 
    "root": {
        "level": app_log_level,
        "handlers": ["file_handler", "console_handler"]
    }
	}
	dictConfig(dictconfig)

