import requests
import json
import time
import MySQLdb

from .constants import NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS


class TradesManager:
    def __init__(self, mysql_table, url):
        self.db = MySQLdb.connect('localhost', 'monitor2', 'password', 'crypto_trades')
        self.curs = self.db.cursor()
        self.mysql_table = mysql_table

        self.url = url

        self.insert_stmt = (f'INSERT IGNORE INTO {self.mysql_table} '
                            f'(unixRequestTime, unixReturnTime, trade_time, amount, price, type, tid) '
                            f'Values (%s, %s, %s, %s, %s, %s, %s)')

    def get_trades(self):
        request_time, return_time, result = self._get_request_trades()

        for row in result:
            self.add_row_to_mysql(request_time, return_time, row)

    def add_row_to_mysql(self, request_time, return_time, row):
        trade_ts = row['date_ms']*MILLISECONDS_TO_NANOSECONDS
        insert_query = (f'INSERT IGNORE INTO {self.mysql_table} '
                        f'(unixRequestTime, unixReturnTime, trade_time, amount, price, side, tid) '
                        f'Values ({request_time}, {return_time}, {trade_ts}, {row["amount"]}, '
                        f'{row["price"]}, {row["type"]}, {row["tid"]})')

        self.curs.execute(insert_query)
        self.db.commit()

    def _get_request_trades(self,):
        request_time = int(time.time()*NANOSECOND_FACTOR)
        result = (requests.get(self.url, timeout=15)).json()
        return_time = int(time.time()*NANOSECOND_FACTOR)
        return request_time, return_time, result

    def __del__(self):
        self.db.close()
