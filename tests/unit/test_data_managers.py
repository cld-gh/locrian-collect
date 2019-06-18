"""
Test Data Managers.
"""
import logging

import requests
import pytest

from locrian_collect.data_managers import BaseManager


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
    def test_request_data_logs(self, error_type, error_msg, mocker, caplog):
        """Test logs for request_data when an exception occurs."""
        logger = logging.getLogger()
        mocker.patch('locrian_collect.data_managers.logger_order_book', logger)

        mocker.patch('locrian_collect.data_managers.requests.get', side_effect=error_type)

        base_manager = BaseManager('test_table', 'test_url', 'test_name')
        result = base_manager._request_data()

        assert caplog.record_tuples[0][2] == error_msg
        assert result == (None, None, None)
#
#
# class TestOrderBookManager:
#     """Tests for OrderBookManager."""
#
#     @patch('locrian_collect.data_managers.BaseManager.add_row_to_mysql')
#     def test_filter_results_success(self, mock_add_row, patch_database_and_requests, mock_book):
#         """Test filter results performs the desired checks."""
#         order_book_manager = OrderBookManager('test_asset_name', 'test_table', 'url')
#         order_book_manager.filter_results(1, 1, mock_book)
#         mock_add_row.assert_called_with(1, 1, f"'{json.dumps(mock_book)}'")
#
#     @patch('locrian_collect.data_managers.logger_order_book')
#     @pytest.mark.parametrize('data', (
#         {'test': 1},
#         {'ask': [0, 1]},
#         {'bid': [0, 1]},
#         {'ask': [0, 1], 'bid': []},
#         {'ask': [], 'bid': [0, 1]}
#     ))
#     def test_filter_results_incorrect_format(self, mock_logger, data, patch_database_and_requests):
#         """Test filter results performs the desired checks."""
#         order_book_manager = OrderBookManager('test_asset_name', 'test_table', 'url')
#         order_book_manager.filter_results(1, 1, data)
#         mock_logger.warn.assert_called_with(f'Error test_table: \'{json.dumps(data)}\'')
#
#         assert 'Error test_table' in mock_logger.warn._mock_call_args_list[-1][0][0]
