# """
# Test the scheduling of collecting data.
# """
# import pytest
#
# from locrian_collect.scheduler import scheduler, delta_time_to_sleep
#
#
# class MockLogger:
#     """Mock the logger"""
#     def __init__(self):
#         pass
#
#     def info(self):
#         """mock info call"""
#
#
# class MockThread:
#     """Mock Thread class."""
#     def __init__(self, target):
#         self.target = target
#
#     def start(self):
#         """Mock starting thread."""
#
#     def join(self):
#         """Mock joining thread."""
#
#
# class MockTime:
#     """Mock time namespace."""
#     def __init__(self):
#         self.num_called = 0
#
#     def sleep(self, *args):
#         """Mock sleep"""
#         if self.num_called > 2:
#             raise Exception('This is to break out of infinite loop in testing.')
#         else:
#             self.num_called += 1
#
#     def time(self):
#         """Mock current time."""
#         return 1
#
#
# def test_scheduler(monkeypatch):
#     """Test scheduler success"""
#     mock_logger = MockLogger()
#     mock_time = MockTime()
#     monkeypatch.setattr('locrian_collect.scheduler.logger_trades', mock_logger)
#     monkeypatch.setattr('locrian_collect.scheduler.Thread', MockThread)
#     monkeypatch.setattr('locrian_collect.scheduler.time', mock_time)
#     with pytest.raises(Exception, match='This is to break out of infinite loop in testing.'):
#         scheduler('trades')
#
#
# def test_scheduler_to_many_requests(monkeypatch):
#     """Test scheduler raises if request rate is too high."""
#     mock_params_map = {'trades': {'managers': [None]*100,
#                                   'time_between_requests': 1,
#                                   'logger': None,
#                                   'log_msg': None,
#                                   'offset': None}}
#     monkeypatch.setattr('locrian_collect.scheduler.PARAMS_MAP', mock_params_map)
#     with pytest.raises(ValueError, match='Number of requests per second exceeds Okcoin.*'):
#         scheduler('trades')
#
#
# @pytest.mark.parametrize('interval, offset, expected', (
#     [10, 0.1, 5.1],
#     [2, 0.03, 1.03]
# ))
# def test_delta_time_to_sleep(interval, offset, expected, monkeypatch, mocker):
#     """Test the time to sleep is as expected."""
#     mocker.patch('locrian_collect.scheduler.time.time', return_value=12345)
#     result = delta_time_to_sleep(interval, offset)
#     assert result == expected
