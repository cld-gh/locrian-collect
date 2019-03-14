import requests
import time
import MySQLdb
import json

from .constants import (CURRENCY_LIST, BASE_URL_SPOT_TRADES, CONTRACT_LIST, BASE_URL_FUTURE_TRADES,
                        BASE_URL_SPOT_DEPTH, BASE_URL_INDEX, BASE_URL_FUTURE_DEPTH,
                        NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS)
from .logs import logger_order_book, logger_index, logger_trades


def _connect_to_mysql(database_name):
    return MySQLdb.connect('localhost', 'monitor2', 'password', database_name,
                           unix_socket='/var/run/mysqld/mysqld.sock')


class BaseManager:
    def __init__(self, mysql_table, url, database_name):
        self.database_name = database_name
        self.db = _connect_to_mysql(self.database_name)
        self.curs = self.db.cursor()
        self.mysql_table = mysql_table
        self.url = url
        self.col_name = None

    def get_data(self):
        request_time, return_time, result = self._request_data()

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

    def execute_query(self, query):
        if not self.db.open:
            self.db = _connect_to_mysql(self.database_name)
            self.curs = self.db.cursor()
        return self.curs.execute(query)

    def _request_data(self):
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


class OrderBookManager(BaseManager):
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url, database_name='bitcoindb_V2')
        self.col_name = 'orderBook'

    def filter_results(self, request_time, return_time, result):
        result = f"'{json.dumps(result)}'"

        if 'ask' not in f'{result}' or 'bid' not in f'{result}' or '[]' in f'{result}':
            logger_order_book.warn(f'Error {self.mysql_table}: {result}')
        else:
            self.add_row_to_mysql(request_time, return_time, result)


class IndexManager(BaseManager):
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url, database_name='bitcoindb_V2')
        self.col_name = 'future_index'

    def filter_results(self, request_time, return_time, result):
        try:
            result = result['future_index']
        except KeyError:
            logger_index.warn(f'Error {self.mysql_table}: {result}')
            return

        self.add_row_to_mysql(request_time, return_time, result)


class TradesManager(BaseManager):
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url, database_name='crypto_trades')

    def get_data(self):
        request_time, return_time, result = self._request_data()

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

    def filter_results(self, *args):
        # Trades Manager does not use this function
        raise NotImplementedError


def trades_url_mysql_maps():
    assets = []
    for currency in CURRENCY_LIST:
        assets.append({'mysql_table': f'trades_spot_{currency}',
                       'url': f'{BASE_URL_SPOT_TRADES}?symbol={currency}_usd'})
        for contract in CONTRACT_LIST:
            assets.append({'mysql_table': f'trades_future_{contract}_{currency}',
                           'url': f'{BASE_URL_FUTURE_TRADES}?symbol={currency}_usd&contract_type={contract}'})
    return assets


def get_trades_managers():
    assets = trades_url_mysql_maps()

    trades_managers = []
    for asset in assets:
        trades_managers.append(TradesManager(**asset))

    return trades_managers


def get_managers():
    managers = []
    for currency in CURRENCY_LIST:
        managers.append(OrderBookManager(mysql_table=f'spot_{currency}_usd_orderbook',
                                         url=f'{BASE_URL_SPOT_DEPTH}?symbol={currency}_usd'))

        managers.append(IndexManager(mysql_table=f'future_index_{currency}_usd',
                                     url=f'{BASE_URL_INDEX}?symbol={currency}_usd'))

        for contract in CONTRACT_LIST:
            url = f'{BASE_URL_FUTURE_DEPTH}?symbol={currency}_usd&contract_type={contract}&size=50'
            managers.append(OrderBookManager(mysql_table=f'future_{currency}_usd_{contract}_orderbook', url=url))

    return managers
