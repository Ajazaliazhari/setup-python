from  mysql_generic import *
import settings


def db_con(conn):
    try:
        if conn:
            print("==> Database connection is established <==")
            #lg.logw("error",f"{log_str}  Database connection established\n")
        else:
            conn = MySQL_connector(address = '{}'.format(settings.info['host']),user='{}'.format(settings.info['username']),password='{}'.format(settings.info['password']),database='{}'.format(settings.info['db']))
    except Exception as e:
        print("===<Raise error on db connection>==",e)
    return conn
