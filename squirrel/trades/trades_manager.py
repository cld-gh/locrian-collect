import time
import requests
import MySQLdb

from .constants import NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS
from ..squirrel_logging import logger


class TradesManager:
    def __init__(self, mysql_table, url):
        self.db = MySQLdb.connect('localhost', 'monitor2', 'password', 'crypto_trades',
                                  unix_socket='/var/run/mysqld/mysqld.sock')
        self.curs = self.db.cursor()
        self.mysql_table = mysql_table
        self.url = url

    def get_trades(self):
        request_time, return_time, result = self._get_request_trades()

        if result is None:
            return

        for row in result:
            if self.check_tid(row['tid']):
                continue
            self.add_row_to_mysql(request_time, return_time, row)

    def check_tid(self, tid):
        query = f'SELECT tid FROM {self.mysql_table} where tid={tid}'
        return self.curs.execute(query)

    def add_row_to_mysql(self, request_time, return_time, row):
        trade_ts = row['date_ms']*MILLISECONDS_TO_NANOSECONDS
        insert_query = (f'INSERT IGNORE INTO {self.mysql_table} '
                        f'(unixRequestTime, unixReturnTime, trade_time, amount, price, side, tid) '
                        f'Values ({request_time}, {return_time}, {trade_ts}, {row["amount"]}, '
                        f'{row["price"]}, "{row["type"]}", {row["tid"]})')
        self.curs.execute(insert_query)
        self.db.commit()

    def _get_request_trades(self,):
        try:
            request_time = int(time.time() * NANOSECOND_FACTOR)
            result = (requests.get(self.url, timeout=15)).json()
            return_time = int(time.time() * NANOSECOND_FACTOR)
            return request_time, return_time, result

        except requests.Timeout:
            logger.warn(f'Timeout error: {self.mysql_table}')
        except RuntimeError:
            logger.warn(f'Runtime error: {self.mysql_table}')
        except ValueError as exc:
            logger.warn(exc)

        return None, None, None


    def __del__(self):
        self.db.close()
