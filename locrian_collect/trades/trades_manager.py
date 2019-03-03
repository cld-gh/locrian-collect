import time
import requests
import MySQLdb

from locrian_collect.constants import NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS
from ..locrian_collect_logging import logger_trades


def _connect_to_mysql():
    return MySQLdb.connect('localhost', 'monitor2', 'password', 'crypto_trades',
                           unix_socket='/var/run/mysqld/mysqld.sock')


class TradesManager:
    def __init__(self, mysql_table, url):
        self.db = _connect_to_mysql()
        self.curs = self.db.cursor()
        self.mysql_table = mysql_table
        self.url = url

    def get_trades(self):
        request_time, return_time, result = self._get_request_trades()

        if result is None:
            return

        for row in result:
            try:
                if self.check_tid(row['tid']):
                    continue
            except KeyError as ex:
                logger_trades.warn(f'Error no tid, {row}  |  {ex}')
                continue
            except TypeError as ex:
                logger_trades.warn(f'Error - tid type {row} | {ex}')
            self.add_row_to_mysql(request_time, return_time, row)

    def check_tid(self, tid):
        query = f'SELECT tid FROM {self.mysql_table} where tid={tid}'
        return self.execute_query(query)

    def add_row_to_mysql(self, request_time, return_time, row):
        trade_ts = row['date_ms']*MILLISECONDS_TO_NANOSECONDS
        insert_query = (f'INSERT IGNORE INTO {self.mysql_table} '
                        f'(unixRequestTime, unixReturnTime, trade_time, amount, price, side, tid) '
                        f'Values ({request_time}, {return_time}, {trade_ts}, {row["amount"]}, '
                        f'{row["price"]}, "{row["type"]}", {row["tid"]})')
        self.execute_query(insert_query)
        self.db.commit()

    def execute_query(self, query):
        if not self.db.open:
            self.db = _connect_to_mysql()
            self.curs = self.db.cursor()
        return self.curs.execute(query)

    def _get_request_trades(self,):
        try:
            request_time = int(time.time() * NANOSECOND_FACTOR)
            result = (requests.get(self.url, timeout=15)).json()
            return_time = int(time.time() * NANOSECOND_FACTOR)
            return request_time, return_time, result

        except requests.Timeout:
            logger_trades.warn(f'Timeout error: {self.mysql_table}')
        except RuntimeError:
            logger_trades.warn(f'Runtime error: {self.mysql_table}')
        except ValueError as exc:
            logger_trades.warn(exc)

        return None, None, None
