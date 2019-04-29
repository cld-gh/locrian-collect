"""
Data Managers that control the collection and storage of data from an exchange to a database.
"""
import time
import json
import requests
import MySQLdb
import pandas as pd
import sqlalchemy

from .constants import (CURRENCY_LIST, BASE_URL_SPOT_TRADES, CONTRACT_LIST, BASE_URL_FUTURE_TRADES,
                        BASE_URL_SPOT_DEPTH, BASE_URL_INDEX, BASE_URL_FUTURE_DEPTH,
                        NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS,
                        Side, ORDER_MAP)
from .logs import logger_order_book, logger_index, logger_trades


def _connect_to_mysql(database_name):
    """Connect to a mysql database

    Parameters
    ----------
    database_name: str
        Name of the database to connect to.

    Returns
    -------
    Connection to MySQLdb.

    """
    return MySQLdb.connect('localhost', 'monitor2', 'password', database_name,
                           unix_socket='/var/run/mysqld/mysqld.sock')


def _get_sqlalchemy_engine(database_name='locrian_level_two'):
    """Get Sqlalchemy engine

    Parameters
    ----------
    database_name: str
        Name of the database to connect to.

    Returns
    -------
    Engine for sqlalchemy.

    """
    return sqlalchemy.create_engine(f'mysql://monitor2:password@localhost:3306/{database_name}?unix_socket=/var/run/mysqld/mysqld.sock')


class BaseManager:
    """Base class for managers.

    Parameters
    ----------
    mysql_table: str
        The name of the table to connect with.
    url: str
        The url of the exchange to connect with.
    database_name: str
        The name of the database.

    """
    def __init__(self, mysql_table, url, database_name):
        self.database_name = database_name
        self.db = _connect_to_mysql(self.database_name)
        self.curs = self.db.cursor()
        self.mysql_table = mysql_table
        self.url = url
        self.col_name = None

    def get_data(self):
        """Helper function to get data and save the results after filtering."""
        request_time, return_time, result = self._request_data()

        if result is None:
            return

        self.filter_results(request_time, return_time, result)

    def filter_results(self, request_time, return_time, result):
        """Filter results not implemented in the base class."""
        raise NotImplementedError

    def execute_query(self, query):
        """Execute a database query.

        Parameters
        ----------
        query: str
            A SQL query to be executed

        Returns
        -------
        Result of the SQL query.
        """
        if not self.db.open:
            self.db = _connect_to_mysql(self.database_name)
            self.curs = self.db.cursor()
        return self.curs.execute(query)

    def _request_data(self):
        """Make a REST request to the url."""
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
            logger_order_book.warn(f'{exc}')

        return None, None, None


class OrderBookManager(BaseManager):
    """Manager for collecting and saving order book data between an exchange and a database.

    Parameters
    ----------
    mysql_table: str
        Name of the database table to connect with.
    url: str
        The exchanges url for requesting data.
    """
    def __init__(self, asset_name, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url, database_name='bitcoindb_V2')
        self.col_name = 'orderBook'
        self.asset_name = asset_name

    def filter_results(self, request_time, return_time, result):
        """Filter the results and save the result to the database.

        If the data does not have both bids and asks data do not record anything and log a warning.

        Parameters
        ----------
        request_time: int
            The unix time in nanoseconds the request was made.
        return_time: int
            The unix time in nanoseconds the data was returned from the request.
        result: The data returned by a REST request.
        """
        book = result
        result = f"'{json.dumps(result)}'"

        if 'ask' not in f'{result}' or 'bid' not in f'{result}' or '[]' in f'{result}':
            logger_order_book.warn(f'Error {self.mysql_table}: {result}')
        else:
            self.add_book_to_db(request_time, book)

    def add_book_to_db(self, timestamp, book):
        """Add level two book to database.

        Parameters
        ----------
        timestamp: int
            The unix time in nanoseconds the request was made.
        book: dict
            The level two book as a dict; {'side': [price, volume]}
        """
        level_two_book = _parse_level_two_book(timestamp, book)
        engine = _get_sqlalchemy_engine('locrian_level_two')
        level_two_book.to_sql(self.asset_name, engine, if_exists='append', index=False)
        engine.dispose()


def _parse_level_two_book(timestamp, book):
    """Parse level two book from json returned by exchange to level two dataframe.

    Parameters
    ----------
    timestamp: int
        The unix time in nanoseconds the request was made.
    book: dict
        The level two book as a dict; {'side': [price, volume]}

    Returns
    -------
    pd.DataFame
        Level two book as a pandas dataframe.
    """
    data = []
    for side in book:
        try:
            ordering = ORDER_MAP[Side[side]]
        except KeyError as key_error:
            print(side, book, timestamp)
            raise KeyError(key_error)

        levels = book[side][::ordering]
        side_as_value = Side[side].value

        for level_index, level in enumerate(levels, 1):
            price = level[0]
            volume = level[1]
            data.append([timestamp, side_as_value, level_index, price, volume])

    return pd.DataFrame(data, columns=['timestamp', 'side', 'level', 'price', 'volume'])


class IndexManager(BaseManager):
    """Manager for collecting and saving futures index data between an exchange and a database.

    Parameters
    ----------
    mysql_table: str
        Name of the database table to connect with.
    url: str
        The exchanges url for requesting data.
    """
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url, database_name='bitcoindb_V2')
        self.col_name = 'future_index'

    def add_row_to_mysql(self, request_time, return_time, row):
        """Insert a row into the mysql database.

        Parameters
        ----------
        request_time: int
            The unix time in nanoseconds the request was made.
        return_time: int
            The unix time in nanoseconds the data was returned from the request.
        row: str
            A json formatted string of the data to record.
        """
        insert_query = (f'INSERT INTO {self.mysql_table} '
                        f'(unixRequestTime, unixReturnTime, {self.col_name}) '
                        f"Values ({request_time}, {return_time}, {row})")
        self.execute_query(insert_query)
        self.db.commit()

    def filter_results(self, request_time, return_time, result):
        """Filter the results and save the result to the database.

        If the data does not have future_index as a key do not save the data.

        Parameters
        ----------
        request_time: int
            The unix time in nanoseconds the request was made.
        return_time: int
            The unix time in nanoseconds the data was returned from the request.
        result: The data returned by a REST request.
        """
        try:
            result = result['future_index']
        except KeyError:
            logger_index.warn(f'Error {self.mysql_table}: {result}')
            return

        self.add_row_to_mysql(request_time, return_time, result)


class TradesManager(BaseManager):
    """Manager for collecting and saving trades data between an exchange and a database.

    Parameters
    ----------
    mysql_table: str
        Name of the database table to connect with.
    url: str
        The exchanges url for requesting data.
    """
    def __init__(self, mysql_table, url):
        super().__init__(mysql_table=mysql_table, url=url, database_name='crypto_trades')

    def get_data(self):
        """Override the BaseManager method. Get data from the exchange and check if that data.
        Already exists in the database before saving."""

        request_time, return_time, result = self._request_data()

        if result is None:
            return

        for row in result:
            try:
                if self.check_tid(row['tid']):
                    continue
            except KeyError as exception_msg:
                logger_trades.warn(f'Error no tid, {row}  |  {exception_msg}')
                continue
            except TypeError as exception_msg:
                logger_trades.warn(f'Error - tid type {row} | {exception_msg}')
            self.add_row_to_mysql(request_time, return_time, row)

    def check_tid(self, tid):
        """Get all the trade identifiers from the database."""
        query = f'SELECT tid FROM {self.mysql_table} where tid={tid}'
        return self.execute_query(query)

    def add_row_to_mysql(self, request_time, return_time, row):
        """Insert a row into the mysql database.

        Parameters
        ----------
        request_time: int
            The unix time in nanoseconds the request was made.
        return_time: int
            The unix time in nanoseconds the data was returned from the request.
        row: dict
            Dictionary of the data columns to save.
        """
        trade_ts = row['date_ms']*MILLISECONDS_TO_NANOSECONDS
        insert_query = (f'INSERT IGNORE INTO {self.mysql_table} '
                        f'(unixRequestTime, unixReturnTime, trade_time, amount, price, side, tid) '
                        f'Values ({request_time}, {return_time}, {trade_ts}, {row["amount"]}, '
                        f'{row["price"]}, "{row["type"]}", {row["tid"]})')
        self.execute_query(insert_query)
        self.db.commit()

    def filter_results(self, request_time, return_time, result):
        """Trades Manager does not use this function"""
        raise NotImplementedError


def trades_url_mysql_maps():
    """Return a list of dicts where the dicts have information for the mysql_table and url
    to get the data."""
    assets = []
    for currency in CURRENCY_LIST:
        assets.append({'mysql_table': f'trades_spot_{currency}',
                       'url': f'{BASE_URL_SPOT_TRADES}?symbol={currency}_usd'})
        for contract in CONTRACT_LIST:
            assets.append({
                'mysql_table':
                    f'trades_future_{contract}_{currency}',
                'url':
                    f'{BASE_URL_FUTURE_TRADES}?symbol={currency}_usd&contract_type={contract}'})
    return assets


def get_trades_managers():
    """Get a list of Trades Managers"""
    assets = trades_url_mysql_maps()

    trades_managers = []
    for asset in assets:
        trades_managers.append(TradesManager(**asset))

    return trades_managers


def get_managers():
    """Get a list of Managers for order books and future indexes."""
    managers = []
    for currency in CURRENCY_LIST:
        managers.append(OrderBookManager(asset_name=f'spot_{currency}',
                                         mysql_table=f'spot_{currency}_usd_orderbook',
                                         url=f'{BASE_URL_SPOT_DEPTH}?symbol={currency}_usd'))

        managers.append(IndexManager(mysql_table=f'future_index_{currency}_usd',
                                     url=f'{BASE_URL_INDEX}?symbol={currency}_usd'))

        for contract in CONTRACT_LIST:
            url = f'{BASE_URL_FUTURE_DEPTH}?symbol={currency}_usd&contract_type={contract}&size=200'
            managers.append(
                OrderBookManager(asset_name=f'future_{currency}_{contract}',
                                 mysql_table=f'future_{currency}_usd_{contract}_orderbook',
                                 url=url))

    return managers
