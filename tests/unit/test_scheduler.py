"""
Test the scheduling of collecting data.
"""
from locrian_collect.scheduler import delta_time_to_sleep


def test_delta_time_to_sleep(interval, offset, expected, monkeypatch, mocker):
    """Test the time to sleep is as expected."""
    mocker.patch('locrian_collect.scheduler.time.time', return_value=12345)
    result = delta_time_to_sleep(interval, offset)
    assert result == expected
