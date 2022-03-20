
import logging, os
from common.dbconnector import dbconnector


class FileUtils:

    def __init__(self):
        logging.debug('Instanciating File Utils')

    def __del__(self):
        logging.debug('Destructing File Utils')

    def file_to_string(self, fullpath):
        with open(fullpath, 'r') as file_handler:
            str_res = file_handler.read()
        return str_res

    def get_files_from_folder(self, folder_fullpath, extension=None):
        # assign directory
        directory = folder_fullpath
        res = []
        # iterate over files in
        # that directory
        for root, dirs, files in os.walk(directory):
            for filename in files:
                logging.debug(f'filename found {filename}')
                if extension is not None and filename.lower().endswith(extension):
                    res.append(os.path.join(root, filename)) 
                elif extension is None:
                    res.append(os.path.join(root, filename))
        return res