#!/usr/bin/env python3
import logging
import pymysql as sql
import time

log = logging.getLogger('MySQL')
class MySQL_connector():
    def  __init__(self, address='', user='', password='', database='',
                    reconnect_retries = 10):
        self.address        = address
        self.user           = user
        self.password       = password
        self.database       = database
        self.is_connected   = 0
        self.max_retries    = reconnect_retries
        self._initialize_connection()

    def _initialize_connection(self):
        self.is_connected = 0
        self.conn  = sql.connect(self.address,
                                self.user,
                                self.password,
                                self.database
                                )
        self.cur = self.conn.cursor()
        self.cur.execute('select 1')
        self.is_connected = 1

    def _reconnect(self):
        current_try = 0
        while current_try < self.max_retries:
            try:
                self._initialize_connection()
                if self.is_connected:
                    return
            except (sql.err.OperationalError, sql.err.InternalError):
                current_try += 1
            except Exception as e:
                raise Exception('Unable to reconnect', str(e))
            time.sleep(.5)
        raise Exception('Unable to reconnect')

    def insert_query(self, query, flag=None, query_retry=3):
        qtry = 0
        self.result = 0
        executed = 0
        while qtry < query_retry and executed == 0:
            try:
                self.cur.execute(query)
                self.conn.commit()
                executed = 1
                rowid = self.cur.lastrowid
            except (sql.err.OperationalError, sql.err.InternalError):
                qtry += 1
                self._reconnect()
            except:
                return 0
            else:
                log.info("INSERT success")
                print("==this print from mysql_generic==    and print rowid=======",rowid)  
                return rowid;#,self.result

    def select_query(self, query, flag=None, query_retry=3):
        qtry = 0
        executed = 0
        self.result = 0 
        log.info("Flag: {}".format(flag))

        while qtry < query_retry and executed == 0:
            try:
                num_of_rows = self.cur.execute(query)
                self.conn.commit()
                if flag.lower() == 'multi':
                    result = self.cur.fetchall()
                    self.conn.commit()
                    executed = 1
                elif flag.lower() == 'single':
                    result = self.cur.fetchone()
                    self.conn.commit()
                    executed = 1
            except (sql.err.OperationalError, sql.err.InternalError):
                qtry += 1
                self._reconnect()
            except:
                return 0
        if qtry == query_retry:
            raise Exception('the retry limit exceeded')
        return result

    def delete_query(self, query, flag=None, query_retry=3):
        qtry = 0
        executed = 0
        while executed == 0 and qtry < query_retry:
            try:
                self.cur.execute(query)
                self.conn.commit()
                executed = 1
            except (sql.err.OperationalError, sql.err.InternalError):
                self._reconnect()
                qtry += 1
            except:
                return 0
            else:
                log.info('DELETE success')

    def update_query(self, query,flag=None, query_retry=3):
        qtry = 0
        executed = 0
        while executed == 0 and qtry < query_retry:
            try:
                self.cur.execute(query)
                self.conn.commit()
                executed = 1
            except (sql.err.OperationalError, sql.err.InternalError):
                self._reconnect()
                qtry += 1
            except:
                return 0
            else:
                log.info('update success')

    def alter_query(self, query, flag=None, query_retry=3):
        qtry = 0
        self.result = 0
        executed = 0
        while executed == 0 and qtry < query_retry:
            try:
                self.result = self.cur.execute(query)
                self.conn.commit()
                executed = 1
            except (sql.err.OperationalError, sql.err.InternalError):
                self._reconnect()
                qtry += 1
            except:
                return 0
            else:
                log.infoi('alter success')
                return result

    def create_query(self, query, flag=None, query_retry=3):
        qtry = 0
        self.result = 0
        executed = 0
        while executed == 0 and qtry < query_retry:
            try:
                self.result = self.cur.execute(query)
                self.conn.commit()
                executed = 1
            except (sql.err.OperationalError, sql.err.InternalError):
                self._reconnect()
                qtry += 1
            except:
                return 0
            else:
                log.info('create table success')
                return result
    def close(self):
        self.cur.close()
        self.conn.close()
