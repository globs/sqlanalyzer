
import logging, os
from common.dbconnector import dbconnector
from pathlib import Path

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

    def file_size(self, filefullpath):
        return Path(filefullpath).stat().st_size

    # Python code to find frequency of each word
    def word_freq(self, str):
    
        # break the string into list of words 
        cleaned_str =str.replace("'", " ").replace(",", " ").replace(";", " ").replace("(", " ").replace(")", " ")
        words = cleaned_str.split()         
        res = []
        frequencies = []
        # loop till string values present in list str
        for i in words:             
            # checking for the duplicacy
            if i not in res:
                # insert value in str2
                res.append(i) 
        for i in range(0, len(res)):
            # count the frequency of each word(present 
            # in str2) in str and print
            frequencies.append({ "word": res[i], "freq" : str.count(res[i])})  
        return frequencies