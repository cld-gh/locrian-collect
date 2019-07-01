"""
Test the scheduling of collecting data.
"""
import pytest

from locrian_collect.scheduler import delta_time_to_sleep


@pytest.mark.parametrize('interval, offset, expected', [
    [10, 0.1, 5.1],
    [20, 0.1, 15.1],
    [100, 0.01, 55.01],
])
def test_delta_time_to_sleep(interval, offset, expected, monkeypatch, mocker):
    """Test the time to sleep is as expected."""
    mocker.patch('locrian_collect.scheduler.time.time', return_value=12345)
    result = delta_time_to_sleep(interval, offset)
    assert result == expected
