import logging

from squirrel.constants import BASE_DATA_DIRECTORY


def get_logger(logger_file, log_name):
    """ Sets up the formatting for the h_logger to output to a file and returns the h_logger."""
    h_logger = logging.getLogger(log_name)
    h_logger.setLevel(logging.DEBUG)

    if h_logger.handlers:
        h_logger.handlers = []

    fh = logging.FileHandler(logger_file)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter(f'[%(asctime)s : %(name)s : %(levelname)s]  %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    h_logger.addHandler(fh)
    h_logger.addHandler(ch)

    return h_logger


logger_trades = get_logger(f'{BASE_DATA_DIRECTORY}/squirrel.log', log_name='squirrel_trades')
logger_order_book = get_logger(f'{BASE_DATA_DIRECTORY}/squirrel.log', log_name='squirrel_order_book')
