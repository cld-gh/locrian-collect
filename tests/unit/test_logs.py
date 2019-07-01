"""Test logging"""
import tempfile

from locrian_collect.logs import get_logger


def test_get_logger():
    """Test get_logger returns the expected logger for two calls."""

    with tempfile.NamedTemporaryFile() as temp:
        expected_dir = temp.name
        expected_fmt = '[%(asctime)s : %(name)s : %(levelname)s]  %(message)s'

        loggers = {}

        for index in range(2):
            loggers[index] = get_logger(temp.name, 'test_name')

            assert loggers[index].name == 'test_name'

            assert loggers[index].handlers[0].baseFilename == expected_dir
            assert loggers[index].handlers[1].formatter._fmt == expected_fmt
