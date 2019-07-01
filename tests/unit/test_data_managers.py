"""
Test Data Managers.
"""
import logging

import requests
import pytest

from locrian_collect.data_managers import (
    BaseManager, OrderBookManager, IndexManager, TradesManager,
    trades_url_mysql_maps
)


@pytest.fixture
def patch_requests_get(mocker):
    """Mock requests get.  Either returns data or raises depending on the url."""
    mock_response = mocker.MagicMock()
    mock_response.json.return_value = 1
    mocker.patch('locrian_collect.data_managers.requests.get', return_value=mock_response)
    mocker.patch('locrian_collect.data_managers.time.time', return_value=123)
    yield mock_response


@pytest.fixture
def patch_database(monkeypatch, mocker):
    """Patch the database connection and request calls"""
    mock_engine = mocker.MagicMock()
    mock_create_engine = mocker.patch('locrian_collect.data_managers.sqlalchemy.create_engine', return_value=mock_engine)
    monkeypatch.setattr('locrian_collect.data_managers.DB_USERNAME', 'test_username')
    monkeypatch.setattr('locrian_collect.data_managers.DB_PASSWORD', 'test_password')
    monkeypatch.setattr('locrian_collect.data_managers.requests.get', patch_requests_get)
    yield mock_create_engine, mock_engine


@pytest.fixture
def patch_loggers(mocker):
    logger = logging.getLogger()
    mocker.patch('locrian_collect.data_managers.logger_order_book', logger)
    mocker.patch('locrian_collect.data_managers.logger_index', logger)
    mocker.patch('locrian_collect.data_managers.logger_trades', logger)


class TestBaseManager:
    """Tests for the BaseManager class."""

    def test_get_data(self, patch_database):
        """Test get_data success."""
        base_manager = BaseManager('test_table', 'test_url', 'test_name')
        with pytest.raises(NotImplementedError):
            base_manager.get_data()

    def test_execute_query_write(self, mocker, patch_database):
        """Test execute query write success."""
        mock_create_engine, mock_engine = patch_database

        sql_query = 'test_query'
        sql_url = (
            'mysql://test_username:test_password@localhost:3306/test_name'
            '?unix_socket=/var/run/mysqld/mysqld.sock'
        )

        base_manager = BaseManager('test_table', 'no_data', 'test_name')
        base_manager.execute_query_write(sql_query)

        assert mock_create_engine.call_args == mocker.call(sql_url)
        assert f'{mock_engine.execute.call_args[0][0]}' == sql_query
        assert mock_engine.dispose.call_args == mocker.call()

    def test_execute_query_read(self, mocker, patch_database):
        """Test execute query read success."""
        mock_create_engine, mock_engine = patch_database

        sql_query = 'test_query'
        sql_url = (
            'mysql://test_username:test_password@localhost:3306/test_name'
            '?unix_socket=/var/run/mysqld/mysqld.sock'
        )

        base_manager = BaseManager('test_table', 'no_data', 'test_name')
        base_manager.execute_query_read(sql_query)

        assert mock_create_engine.call_args == mocker.call(sql_url)
        assert f'{mock_engine.execute.call_args[0][0]}' == sql_query
        assert mock_engine.execute().fetchall.call_args == mocker.call()
        assert mock_engine.dispose.call_args == mocker.call()

    def test_request_data_success(self, patch_requests_get):
        """Test success of request_data."""
        patch_requests_get.json.return_value = 1
        base_manager = BaseManager('test_table', 'test_url', 'test_name')
        result = base_manager._request_data()
        assert result == (123000000000, 123000000000, 1)

    @pytest.mark.parametrize('error_type, error_msg', [
        [requests.Timeout, 'Timeout error: test_table'],
        [ValueError('value_error'), 'value_error'],
        [RuntimeError, 'Runtime error: test_table']
    ])
    def test_request_data_logs(self, error_type, error_msg, mocker, patch_loggers, caplog):
        """Test logs for request_data when an exception occurs."""

        mocker.patch('locrian_collect.data_managers.requests.get', side_effect=error_type)
        base_manager = BaseManager('test_table', 'test_url', 'test_name')
        result = base_manager._request_data()

        assert caplog.record_tuples[0][2] == error_msg
        assert result == (None, None, None)


class TestOrderBookManager:
    """Tests for OrderBookManager."""

    def test_get_data(self, mocker, patch_database, mock_book, patch_requests_get, patch_loggers):
        """Test get data and saving to database."""
        _, mock_engine = patch_database
        mock_return_df = mocker.Mock()
        mock_parse = mocker.patch('locrian_collect.data_managers.parse_level_two_book', return_value=mock_return_df)
        patch_requests_get.json.return_value = mock_book
        order_book_manager = OrderBookManager('test_table', 'test_url', 'test_name')
        order_book_manager.get_data()
        assert mock_parse.call_args == mocker.call(123000000000, mock_book)
        assert mock_return_df.to_sql.call_args == mocker.call('test_table',
                                                              mock_engine,
                                                              if_exists='append',
                                                              index=False)

    def test_get_data_one_side_of_book_missing(self, patch_database, patch_requests_get, patch_loggers, caplog):
        """Test get data and logging error when one side of the book is missing"""
        patch_requests_get.json.return_value = {'ask': []}
        order_book_manager = OrderBookManager('test_table', 'test_url', 'test_name')
        order_book_manager.get_data()
        assert caplog.record_tuples[0][2] == 'Error test_url: \'{"ask": []}\''

    def test_get_data_no_result(self, patch_database, patch_requests_get, patch_loggers, caplog):
        """Test get data and no saving when result is None"""
        patch_requests_get.json.return_value = None
        order_book_manager = OrderBookManager('test_table', 'test_url', 'test_name')
        assert order_book_manager.get_data() is None
        assert caplog.record_tuples == []


class TestIndexManager:
    """Tests for IndexManager."""

    def test_get_data(self, mocker, patch_requests_get, patch_loggers):
        """Test get data and saving to database."""
        mock_execute_query = mocker.patch('locrian_collect.data_managers.IndexManager.execute_query_write')
        patch_requests_get.json.return_value = {'future_index': [1]}
        index_manager = IndexManager('test_table', 'test_url')
        index_manager.get_data()

        assert mock_execute_query.call_args == mocker.call(
            ('INSERT INTO test_table (unixRequestTime, unixReturnTime, future_index) '
             'Values (123000000000, 123000000000, [1])'))

    def test_get_data_key_error(self, patch_requests_get, patch_loggers, caplog):
        """Test get data and logging key error when future_index not in result."""
        patch_requests_get.json.return_value = {'wrong_key': [1]}
        index_manager = IndexManager('test_table', 'test_url')
        index_manager.get_data()
        assert caplog.record_tuples[0][2] == "Error test_table: {'wrong_key': [1]}"

    def test_get_data_result_is_none(self, patch_requests_get, patch_loggers, caplog):
        """Test get data and no additional log messages are present when there is an error owing to collecting
        data, such as a Timeout error.  The error message will be handled by _request_data method."""
        patch_requests_get.json.return_value = None
        index_manager = IndexManager('test_table', 'test_url')
        index_manager.get_data()
        assert caplog.record_tuples == []


class TestTradeManager:
    """Tests for TradeManager."""

    def test_get_data(self, mocker, patch_requests_get, patch_loggers):
        """Test get data and saving to database."""
        mock_write = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_write')
        mock_read = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_read', return_value=False)
        patch_requests_get.json.return_value = [
            {'tid': 1, 'amount': 1000, 'date_ms': 123000000, 'price': 456, 'type': 'buy'}]
        trade_manager = TradesManager('test_table', 'test_url')
        trade_manager.get_data()
        assert mock_write.call_args == mocker.call(
            ('INSERT IGNORE INTO test_table (unixRequestTime, unixReturnTime, trade_time, '
             'amount, price, side, tid) Values (123000000000, 123000000000, 123000000000000, '
             '1000, 456, "buy", 1)'))

    def test_get_data_trade_id_already_saved(self, mocker, patch_requests_get, patch_loggers):
        """Test get data and saving to database."""
        mock_write = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_write')
        mock_read = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_read', return_value=True)
        patch_requests_get.json.return_value = [
            {'tid': 1, 'amount': 1000, 'date_ms': 123000000, 'price': 456, 'type': 'buy'}]
        trade_manager = TradesManager('test_table', 'test_url')
        trade_manager.get_data()
        assert not mock_write.called

    def test_get_data_no_trade_id(self, mocker, patch_requests_get, patch_loggers, caplog):
        """Test get data and saving to database."""
        mock_write = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_write')
        mock_read = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_read', return_value=True)
        patch_requests_get.json.return_value = [
            {'no_tid': 1}]
        trade_manager = TradesManager('test_table', 'test_url')
        trade_manager.get_data()
        assert not mock_write.called
        assert caplog.record_tuples[0][2] == "Error no tid, {'no_tid': 1}  |  'tid'"

    def test_get_data_row_wrong_type(self, mocker, patch_requests_get, patch_loggers, caplog):
        """Test get data and saving to database."""
        mock_write = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_write')
        mock_read = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_read', return_value=True)
        patch_requests_get.json.return_value = [[]]
        trade_manager = TradesManager('test_table', 'test_url')
        trade_manager.get_data()
        assert not mock_write.called
        assert caplog.record_tuples[0][2] == "Error - tid type [] | list indices must be integers or slices, not str"

    def test_get_data_result_is_none(self, mocker, patch_requests_get, patch_loggers, caplog):
        """Test get data and saving to database."""
        mock_write = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_write')
        mock_read = mocker.patch('locrian_collect.data_managers.TradesManager.execute_query_read', return_value=True)
        patch_requests_get.json.return_value = None
        trade_manager = TradesManager('test_table', 'test_url')
        trade_manager.get_data()
        assert not mock_write.called
        assert caplog.record_tuples == []


def test_trades_url_mysql_maps():
    result = trades_url_mysql_maps()

    expected = [
        {
            'mysql_table': 'trades_spot_btc',
            'url': 'https://www.okcoin.com/api/v1/trades.do?symbol=btc_usd'
        },
        {
            'mysql_table': 'trades_future_this_week_btc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=btc_usd&contract_type=this_week'
        },
        {
            'mysql_table': 'trades_future_next_week_btc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=btc_usd&contract_type=next_week'
        },
        {
            'mysql_table': 'trades_future_quarter_btc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=btc_usd&contract_type=quarter'
        },
        {
            'mysql_table': 'trades_spot_bch',
            'url': 'https://www.okcoin.com/api/v1/trades.do?symbol=bch_usd'
        },
        {
            'mysql_table': 'trades_future_this_week_bch',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=bch_usd&contract_type=this_week'
        },
        {
            'mysql_table': 'trades_future_next_week_bch',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=bch_usd&contract_type=next_week'
        },
        {
            'mysql_table': 'trades_future_quarter_bch',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=bch_usd&contract_type=quarter'
        },
        {
            'mysql_table': 'trades_spot_ltc',
            'url': 'https://www.okcoin.com/api/v1/trades.do?symbol=ltc_usd'
        },
        {
            'mysql_table': 'trades_future_this_week_ltc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=ltc_usd&contract_type=this_week'
        },
        {
            'mysql_table': 'trades_future_next_week_ltc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=ltc_usd&contract_type=next_week'
        },
        {
            'mysql_table': 'trades_future_quarter_ltc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=ltc_usd&contract_type=quarter'
        },
        {
            'mysql_table': 'trades_spot_etc',
            'url': 'https://www.okcoin.com/api/v1/trades.do?symbol=etc_usd'
        },
        {
            'mysql_table': 'trades_future_this_week_etc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=etc_usd&contract_type=this_week'
        },
        {
            'mysql_table': 'trades_future_next_week_etc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=etc_usd&contract_type=next_week'
        },
        {
            'mysql_table': 'trades_future_quarter_etc',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=etc_usd&contract_type=quarter'
        },
        {
            'mysql_table': 'trades_spot_eth',
            'url': 'https://www.okcoin.com/api/v1/trades.do?symbol=eth_usd'
        },
        {
            'mysql_table': 'trades_future_this_week_eth',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=eth_usd&contract_type=this_week'
        },
        {
            'mysql_table': 'trades_future_next_week_eth',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=eth_usd&contract_type=next_week'
        },
        {
            'mysql_table': 'trades_future_quarter_eth',
            'url': 'https://www.okex.com/api/v1/future_trades.do?symbol=eth_usd&contract_type=quarter'
        }
    ]
    assert result == expected