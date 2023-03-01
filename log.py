import logging
import datetime
from logging.handlers import TimedRotatingFileHandler
import os
#import populatehash as PH
global logger
def file_creator():
        global logger
        logger = logging.getLogger("Rotating Log")
        logger.setLevel(logging.INFO)
        flag_hash = {}
        flag_hash['Data'] = 1
        if flag_hash['Data'] == 1:
                logger.disabled = False
        else:
                logger.disabled = True
        handler = TimedRotatingFileHandler("/var/log/czentrix/erp_python/Parsing_indus.log",when="midnight",interval=1,backupCount=5)
        logger.addHandler(handler)

def logw(logtype,message):
        #Checking if erp_managerlog.log file exists or not if file does not exists then it create the file
        list_ = os.listdir('/var/log/czentrix/erp_python/')
        if 'erp_managerlog.log' not in list_:
                file_creator()

        if logtype == 'info':
                logger.info(message)

        elif logtype == 'error':

                logger.error(message,exc_info=True)

        elif logtype == 'warning':

                logger.warning(message)

        elif logtype == 'debug':
                logger.debug(message)
file_creator()
