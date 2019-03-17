"""
Test Data Managers.
"""
import json
from unittest.mock import patch
import requests
import pytest

from locrian_collect.data_managers import BaseManager, OrderBookManager


class MockCursor:
    """Mock database cursor"""
    def __init__(self):
        pass

    @staticmethod
    def execute(query):
        """Mock execute query"""


class MockConnect:
    """Mock database connection"""
    def __init__(self):
        self.open = True

    @staticmethod
    def cursor():
        """Return a mock of a database cursor"""
        return MockCursor()

    @staticmethod
    def commit():
        """Mock commit message to a database."""


def mock_requests_get(url, **kwargs):
    """Mock requests get.  Either returns data or raises depending on the url."""

    class ReqGetReturn:
        """Mock return of request.get"""
        def __init__(self, data):
            self.data = data

        def json(self):
            """Mock of json method to return dict of data."""
            return self.data

    if url == 'no_data':
        return ReqGetReturn(None)
    if url == 'test_url':
        return ReqGetReturn({'test_data': None})
    if url == 'url_raise_timeout':
        raise requests.Timeout
    if url == 'url_raise_runtime':
        raise RuntimeError
    if url == 'url_raise_value_error':
        raise ValueError('Value Error for testing.')


def mock_connect_to_mysql(*args):
    """Return a mock database connection."""
    return MockConnect()


@pytest.fixture
def patch_database_and_requests(monkeypatch):
    """Patch the database connection and request calls"""
    monkeypatch.setattr('locrian_collect.data_managers._connect_to_mysql',
                        mock_connect_to_mysql)
    monkeypatch.setattr('locrian_collect.data_managers.requests.get',
                        mock_requests_get)


class TestBaseManager:
    """Tests for the BaseManager class."""

    def test_get_data(self, patch_database_and_requests):
        """Test get_data success."""
        base_manager = BaseManager('test_table', 'test_url', 'test_name')
        with pytest.raises(NotImplementedError):
            base_manager.get_data()

    def test_get_data_no_data(self, patch_database_and_requests):
        """Test get_data returns None if no data is available."""
        base_manager = BaseManager('test_table', 'no_data', 'test_name')
        assert base_manager.get_data() is None

    def test_add_row_to_mysql(self, patch_database_and_requests):
        """Test adding row to mysql."""
        base_manager = BaseManager('test_table', 'no_data', 'test_name')
        base_manager.add_row_to_mysql('None', 'None', 'None')

    def test_add_row_to_mysql_closed_con(self, patch_database_and_requests):
        """Test adding row to mysql when the connection is closed."""
        base_manager = BaseManager('test_table', 'no_data', 'test_name')
        base_manager.db.open = False
        base_manager.add_row_to_mysql('None', 'None', 'None')

    @patch('locrian_collect.data_managers.logger_order_book')
    @pytest.mark.parametrize('url, error_msg', (
        ['url_raise_runtime', 'Runtime error: test_table'],
        ['url_raise_timeout', 'Timeout error: test_table'],
        ['url_raise_value_error', 'Value Error for testing.']
    ))
    def test_requests_errors_continue(self, mock_logger, url, error_msg,
                                      patch_database_and_requests):
        """Test that when an exception is raised whilst requesting data the method continues."""
        base_manager = BaseManager('test_table', url, 'test_name')
        base_manager._request_data()
        mock_logger.warn.assert_called_with(error_msg)


class TestOrderBookManager:
    """Tests for OrderBookManager."""

    @patch('locrian_collect.data_managers.BaseManager.add_row_to_mysql')
    def test_filter_results_success(self, mock_add_row, patch_database_and_requests):
        """Test filter results performs the desired checks."""
        order_book_manager = OrderBookManager('test_table', 'url')
        data = {'ask': [1], 'bid': [2]}
        order_book_manager.filter_results(1, 1, data)
        mock_add_row.assert_called_with(1, 1, f"'{json.dumps(data)}'")

    @patch('locrian_collect.data_managers.logger_order_book')
    @pytest.mark.parametrize('data', (
        {'test': 1},
        {'ask': [0]},
        {'bid': [0]},
        {'ask': [1], 'bid': []},
        {'ask': [], 'bid': [1]}
    ))
    def test_filter_results_incorrect_format(self, mock_logger, data, patch_database_and_requests):
        """Test filter results performs the desired checks."""
        order_book_manager = OrderBookManager('test_table', 'url')
        order_book_manager.filter_results(1, 1, data)
        mock_logger.warn.assert_called_with(f'Error test_table: \'{json.dumps(data)}\'')

        assert 'Error test_table' in mock_logger.warn._mock_call_args_list[-1][0][0]
