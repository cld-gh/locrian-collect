import requests
import time
import MySQLdb
import json

from ..constants import NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS
from ..squirrel_logging import logger_order_book


def _connect_to_mysql():
    return MySQLdb.connect('localhost', 'monitor2', 'password', 'bitcoindb_V2',
                           unix_socket='/var/run/mysqld/mysqld.sock')


class BaseOrderBookManager:
    def __init__(self, mysql_table, url):
        self.db = _connect_to_mysql()
        self.curs = self.db.cursor()
        self.mysql_table = mysql_table
        self.url = url
        self.col_name = None

    def get_order_book(self):
        request_time, return_time, result = self._get_request_trades()

        if result is None:
            return

        self.filter_results(request_time, return_time, result)

    def filter_results(self, request_time, return_time, result):
        raise NotImplementedError

    def add_row_to_mysql(self, request_time, return_time, row):
        insert_query = (f'INSERT INTO {self.mysql_table} (unixRequestTime, unixReturnTime, {self.col_name}) '
                        f"Values ({request_time}, {return_time}, {row})")
        self.execute_query(insert_query)
        self.db.commit()
        print(self.mysql_table)

    def execute_query(self, query):
        if not self.db.open:
            self.db = _connect_to_mysql()
            self.curs = self.db.cursor()
        return self.curs.execute(query)

    def _get_request_trades(self,):
        try:
            request_time = int(time.time() * NANOSECOND_FACTOR)
            result = (requests.get(self.url, timeout=8)).json()
            return_time = int(time.time() * NANOSECOND_FACTOR)
            return request_time, return_time, result

        except requests.Timeout:
            logger_order_book.warn(f'Timeout error: {self.mysql_table}')
        except RuntimeError:
            logger_order_book.warn(f'Runtime error: {self.mysql_table}')
        except ValueError as exc:
            logger_order_book.warn(exc)

        return None, None, None


class OrderBookManager(BaseOrderBookManager):
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url)
        self.col_name = 'orderBook'

    def filter_results(self, request_time, return_time, result):
        result = f"'{json.dumps(result)}'"

        if 'ask' not in f'{result}' or 'bid' not in f'{result}' or '[]' in f'{result}':
            logger_order_book.warn(f'Error {self.mysql_table}: {result}')

        self.add_row_to_mysql(request_time, return_time, result)


class IndexManager(BaseOrderBookManager):
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url)
        self.col_name = 'future_index'

    def filter_results(self, request_time, return_time, result):
        result = result['future_index']
        self.add_row_to_mysql(request_time, return_time, result)
