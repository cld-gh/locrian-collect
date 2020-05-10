"""
Data Managers that control the collection and storage of data from an exchange to a database.
"""
import json
import time

import requests
import sqlalchemy
from sqlalchemy import text
import pandas as pd

from .constants import (
    NANOSECOND_FACTOR, MILLISECONDS_TO_NANOSECONDS, UNIX_SOCKET, DB_USERNAME, DB_PASSWORD,
    CURRENCY_LIST, CONTRACT_LIST, BASE_OKCOIN_URL, BASE_OKEX_URL
)
from .logs import logger_order_book, logger_index, logger_trades
from .parse_level_two_book import parse_level_two_book


def get_sqlalchemy_engine(database_name):
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
    return sqlalchemy.create_engine(
        f'mysql://{DB_USERNAME}:{DB_PASSWORD}@{host}:3306/{database_name}'
        f'?unix_socket={UNIX_SOCKET}')


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
        """Helper function to get data and save the results after filtering, not implemented
        in the base class."""
        raise NotImplementedError

    def execute_query_read(self, query):
        """Execute a sql query to read from a database

        Parameters
        ----------
        query: str
            SQL query to write to database

        Returns
        -------
        tuple
            Fetches all from sql query.
        """
        engine = get_sqlalchemy_engine(self.database_name)
        result = engine.execute(text(query)).fetchall()
        engine.dispose()
        return result

    def execute_query_write(self, query):
        """Execute a sql query to write to a database

        Parameters
        ----------
        query: str
            SQL query to write to database
        """
        engine = get_sqlalchemy_engine(self.database_name)
        engine.execute(text(query).execution_options(autocommit=True))
        engine.dispose()

    def _request_data(self):
        """Make a REST request to the url."""
        try:
            request_time = int(time.time() * NANOSECOND_FACTOR)
            result = requests.get(self.url, timeout=8).json()
            return_time = int(time.time() * NANOSECOND_FACTOR)
            return request_time, return_time, result

        except requests.Timeout:
            logger_order_book.warning(f'Timeout error: {self.mysql_table}')
        except RuntimeError:
            logger_order_book.warning(f'Runtime error: {self.mysql_table}')
        except ValueError as exc:
            logger_order_book.warning(f'{exc}')

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
        super().__init__(mysql_table=mysql_table, url=url, database_name='locrian_level_two')
        self.col_name = 'orderBook'
        self.asset_name = asset_name

    def get_data(self):
        """Helper function to get data and save the results after filtering."""
        request_time, _, result = self._request_data()

        if result is None:
            return

        book = result
        result = f"'{json.dumps(result)}'"

        if 'ask' not in f'{result}' or 'bid' not in f'{result}' or '[]' in f'{result}':
            logger_order_book.warning(f'Error {self.mysql_table}: {result}')
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
        engine = get_sqlalchemy_engine(self.database_name)
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
        super().__init__(mysql_table=mysql_table, url=url, database_name='locrian_future_index')
        self.col_name = 'future_index'

    def get_data(self):
        """Helper function to get data and save the results after filtering."""
        request_time, return_time, result = self._request_data()

        if result is None:
            return

        try:
            result = result['index']
        except KeyError:
            logger_index.warning(f'Error {self.mysql_table}: {result}')
            return

        self.add_row_to_database(request_time, return_time, result)

    def add_row_to_database(self, request_time, return_time, row):
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
        super().__init__(mysql_table=mysql_table, url=url, database_name='locrian_trades')

    def get_data(self):
        """Override the BaseManager method. Get data from the exchange and check if that data.
        Already exists in the database before saving."""

        request_time, return_time, result = self._request_data()

        if result is None:
            return

        for row in result:
            try:
                if self.check_tid(row['trade_id']):
                    continue
            except KeyError as exception_msg:
                logger_trades.warning(f'Error no trade_id, {row}  |  {exception_msg}')
                continue
            except TypeError as exception_msg:
                logger_trades.warning(f'Error - trade_id type {row} | {exception_msg}')
                continue
            self.add_row_to_database(request_time, return_time, row)

    def add_row_to_database(self, request_time, return_time, row):
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
        trade_ts = pd.Timestamp(row['timestamp']).value
        amount = row.get('size', row.get('qty'))
        if amount is None:
            raise ValueError(f'amount is None.  Cannot get size or qty from {row}')

        insert_query = (f'INSERT IGNORE INTO {self.mysql_table} '
                        f'(unixRequestTime, unixReturnTime, trade_time, amount, price, side, tid) '
                        f'Values ({request_time}, {return_time}, {trade_ts}, {amount}, '
                        f'{row["price"]}, "{row["side"]}", {row["trade_id"]})')
        self.execute_query_write(insert_query)

    def check_tid(self, tid):
        """Get all the trade identifiers from the database.

        Returns
        -------
        list(tuples)
            [(`tid`,), ...]
        """
        query = f'SELECT tid FROM {self.mysql_table} where tid={tid}'
        return self.execute_query_read(query)


def trades_url_mysql_maps():
    """Return a list of dicts where the dicts have information for the mysql_table and url
    to get the data."""
    assets = []
    contract_alias_map = get_future_alias_mapping()

    for currency in CURRENCY_LIST:
        assets.append({'mysql_table': f'trades_spot_{currency}',
                       'url': f'{BASE_OKCOIN_URL}{currency.upper()}-USD/trades?size=200'})
        for contract in CONTRACT_LIST:
            assets.append({
                'mysql_table':
                    f'trades_future_{contract}_{currency}',
                'url':
                    f'{BASE_OKEX_URL}{currency.upper()}-USD-{contract_alias_map[contract]}/trades?size=200'})
    return assets


def get_trades_managers():
    """Get a list of Trades Managers"""
    assets = trades_url_mysql_maps()

    trades_managers = []
    for asset in assets:
        trades_managers.append(TradesManager(**asset))

    return trades_managers


def get_future_alias_mapping():
    aliases = ['this_week', 'next_week', 'quarter']  # Used in pandas query
    data = requests.get('https://www.okex.com/api/futures/v3/instruments').json()
    mapping = (
        pd.DataFrame(data)
        .query('alias in @aliases')[['alias', 'delivery']]
        .drop_duplicates()
        .set_index('alias')
        .to_dict()['delivery']
    )
    return {alias: _parse_date(date) for alias, date in mapping.items()}


def _parse_date(date):
    """2020-01-01 --> 200101"""
    return date.replace('-', '')[2:]


def get_managers():
    """Get a list of Managers for order books and future indexes."""
    managers = []

    contract_alias_map = get_future_alias_mapping()

    for currency in CURRENCY_LIST:
        print(f'{BASE_OKCOIN_URL}{currency.upper()}-USD/book?size=500')
        managers.append(OrderBookManager(asset_name=f'spot_{currency}',
                                         mysql_table=f'spot_{currency}_usd_orderbook',
                                         url=f'{BASE_OKCOIN_URL}{currency.upper()}-USD/book?size=500'))
        print(f'{BASE_OKEX_URL}{currency.upper()}-USD-{contract_alias_map["quarter"]}/index')
        managers.append(
            IndexManager(
                mysql_table=f'future_index_{currency}_usd',
                url=f'{BASE_OKEX_URL}{currency.upper()}-USD-{contract_alias_map["quarter"]}/index'))

        for contract in CONTRACT_LIST:
            url = f'{BASE_OKEX_URL}{currency.upper()}-USD-{contract_alias_map[contract]}/book?size=200'
            print(url)
            managers.append(
                OrderBookManager(asset_name=f'future_{currency}_{contract}',
                                 mysql_table=f'future_{currency}_usd_{contract}_orderbook',
                                 url=url))

    return managers
