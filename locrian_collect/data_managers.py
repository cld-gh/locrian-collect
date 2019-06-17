"""
Data Managers that control the collection and storage of data from an exchange to a database.
"""
import time
import json

import requests
import sqlalchemy
from sqlalchemy import text

from .parse_level_two_book import parse_level_two_book
from .constants import (NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS,
                        UNIX_SOCKET)
from .logs import logger_order_book, logger_index, logger_trades


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
    host = 'localhost'
    user = 'monitor2'
    password = 'password'

    return sqlalchemy.create_engine(
        f'mysql://{user}:{password}@{host}:3306/{database_name}?unix_socket={UNIX_SOCKET}')


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

    def execute_query_write(self, query):
        engine = _get_sqlalchemy_engine(self.database_name)
        engine.execute(text(query).execution_options(autocommit=True))
        engine.dispose()

    def execute_query_read(self, query):
        engine = _get_sqlalchemy_engine(self.database_name)
        result = engine.execute(text(query)).fetchall()
        engine.dispose()
        return result

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
        level_two_book = parse_level_two_book(timestamp, book)
        engine = _get_sqlalchemy_engine('locrian_level_two')
        level_two_book.to_sql(self.asset_name, engine, if_exists='append', index=False)
        engine.dispose()


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
        self.execute_query_write(insert_query)

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
        """Get all the trade identifiers from the database.

        Returns
        -------
        list(tuples)
            [(`tid`,), ...]
        """
        query = f'SELECT tid FROM {self.mysql_table} where tid={tid}'
        return self.execute_query_read(query)

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
        self.execute_query_write(insert_query)

    def filter_results(self, request_time, return_time, result):
        """Trades Manager does not use this function"""
        raise NotImplementedError
