"""
Test Data Managers.
"""
import json
from unittest.mock import patch
import requests
import pytest
import pandas as pd

from locrian_collect.data_managers import BaseManager, OrderBookManager
from locrian_collect.parse_level_two_book import parse_level_two_book


@pytest.fixture
def mock_book():
    return {
        "asks": [
            [4006.41, 0.0574], [4006.37, 0.0574], [4006.34, 0.0574],
            [4006.32, 0.0472], [4005.97, 0.839], [4005.77, 0.0024],
            [4005.73, 0.0405], [4005.56, 0.0112], [4002.83, 22.5915],
            [3999.85, 0.275]
        ],
        "bids": [
            [3999.05, 0.3856], [3999.03, 0.327], [3998.87, 10.8253],
            [3997.63, 1.5318], [3995.81, 0.216], [3995.8, 1.5635],
            [3992.87, 0.203], [3992.85, 1.8945], [3991.86, 0.275],
            [3991.01, 1.8672]
        ]}


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
    def test_filter_results_success(self, mock_add_row, patch_database_and_requests, mock_book):
        """Test filter results performs the desired checks."""
        order_book_manager = OrderBookManager('test_asset_name', 'test_table', 'url')
        order_book_manager.filter_results(1, 1, mock_book)
        mock_add_row.assert_called_with(1, 1, f"'{json.dumps(mock_book)}'")

    @patch('locrian_collect.data_managers.logger_order_book')
    @pytest.mark.parametrize('data', (
        {'test': 1},
        {'ask': [0, 1]},
        {'bid': [0, 1]},
        {'ask': [0, 1], 'bid': []},
        {'ask': [], 'bid': [0, 1]}
    ))
    def test_filter_results_incorrect_format(self, mock_logger, data, patch_database_and_requests):
        """Test filter results performs the desired checks."""
        order_book_manager = OrderBookManager('test_asset_name', 'test_table', 'url')
        order_book_manager.filter_results(1, 1, data)
        mock_logger.warn.assert_called_with(f'Error test_table: \'{json.dumps(data)}\'')

        assert 'Error test_table' in mock_logger.warn._mock_call_args_list[-1][0][0]


def test_parse_level_two_book(mock_book):
    """Test parsing of the level two book."""
    expected = pd.DataFrame({
        'timestamp': {
            0: 12345, 1: 12345, 2: 12345, 3: 12345, 4: 12345,
            5: 12345, 6: 12345, 7: 12345, 8: 12345, 9: 12345,
            10: 12345, 11: 12345, 12: 12345, 13: 12345, 14: 12345,
            15: 12345, 16: 12345, 17: 12345, 18: 12345, 19: 12345},
        'side': {
            0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1, 9: 1,
            10: 2, 11: 2, 12: 2, 13: 2, 14: 2, 15: 2, 16: 2, 17: 2, 18: 2, 19: 2},
        'level': {
            0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9, 9: 10,
            10: 1, 11: 2, 12: 3, 13: 4, 14: 5, 15: 6, 16: 7, 17: 8, 18: 9, 19: 10},
        'price': {
            0: 3999.85, 1: 4002.83, 2: 4005.56, 3: 4005.73, 4: 4005.77,
            5: 4005.97, 6: 4006.32, 7: 4006.34, 8: 4006.37, 9: 4006.41,
            10: 3999.05, 11: 3999.03, 12: 3998.87, 13: 3997.63, 14: 3995.81,
            15: 3995.8, 16: 3992.87, 17: 3992.85, 18: 3991.86, 19: 3991.01},
        'volume': {
            0: 0.275, 1: 22.5915, 2: 0.0112, 3: 0.0405, 4: 0.0024,
            5: 0.839, 6: 0.0472, 7: 0.0574, 8: 0.0574, 9: 0.0574,
            10: 0.3856, 11: 0.327, 12: 10.8253, 13: 1.5318, 14: 0.216,
            15: 1.5635, 16: 0.203, 17: 1.8945, 18: 0.275, 19: 1.8672}})

    assert parse_level_two_book(12345, mock_book).equals(expected)
