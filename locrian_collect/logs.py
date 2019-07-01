"""
Loggers for locrian_collection
"""
import logging

from .constants import BASE_DATA_DIRECTORY


def get_logger(logger_file, log_name):
    """ Sets up the formatting for the h_logger to output to a file and returns the h_logger."""
    h_logger = logging.getLogger(log_name)
    h_logger.setLevel(logging.DEBUG)

    if h_logger.handlers:
        h_logger.handlers = []

    file_handler = logging.FileHandler(logger_file)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(f'[%(asctime)s : %(name)s : %(levelname)s]  %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    h_logger.addHandler(file_handler)
    h_logger.addHandler(stream_handler)

    return h_logger


logger_trades = get_logger(f'{BASE_DATA_DIRECTORY}/locrian_collect.log',
                           log_name='locrian_collect_trades')
logger_order_book = get_logger(f'{BASE_DATA_DIRECTORY}/locrian_collect.log',
                               log_name='locrian_collect_order_book')
logger_index = get_logger(f'{BASE_DATA_DIRECTORY}/locrian_collect.log',
                          log_name='locrian_collect_index')
