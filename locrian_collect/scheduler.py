"""
Schedule the collection of data.
"""
import time
from threading import Thread

from .logs import logger_trades, logger_order_book
from .data_managers import get_trades_managers, get_managers


def schedule_get_order_book_and_index_data():
    """Schedule the recording of order book and index data."""
    db_managers = get_managers()
    logger = logger_order_book
    time_between_requests = 10  # seconds
    offset = 0.001
    log_msg = 'Requesting trades.'
    scheduler(db_managers, logger, time_between_requests, offset, log_msg)


def schedule_get_trades():
    """Schedule the recording of trade data."""
    db_managers = get_trades_managers()
    logger = logger_trades
    time_between_requests = 100  # seconds
    offset = 0.1
    log_msg = 'Requesting order book and futures index.'
    scheduler(db_managers, logger, time_between_requests, offset, log_msg)


def scheduler(db_managers, logger, time_between_requests, offset, log_msg):
    """Schedule the recording of data (order book, index or trades).

    Parameters
    ----------
    db_managers: list
        List of data managers, see data_managers module.
    logger:
        logger object for logging info and warnings.
    time_between_requests: float
        Amount of time to wait between requests of data.
    offset: float
        Offset to add to the amount of time to wait between requests.  If
        the amount of time to wait between requests is 10 and offset 0.1,
        data will be recorded at 0.1, 10.1, 20.1 ... seconds.
    log_msg: str
        Message to display each time data is requested.

    """
    num_managers = len(db_managers)

    if num_managers / time_between_requests > 10:
        raise ValueError("Number of requests per second exceeds Okcoin's "
                         "limits (1 request every 0.1 seconds)")

    while True:
        time.sleep(delta_time_to_sleep(interval=time_between_requests, offset=offset))
        logger.info(log_msg)
        thread_list = []

        for database_manager in db_managers:
            thread_list.append(Thread(target=database_manager.get_data))

        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()

        del thread_list


def delta_time_to_sleep(interval, offset):
    """Get the amount of time to sleep in seconds before making another request.

    Time between now and the next timestamp where the timestamp modulo interval is zero.

    Parameters
    ----------
    interval: float
        The time interval between samples.
    offset: float
        The amount of time extra to sleep for.

    Returns
    -------
    float:
        The amount of time to sleep in seconds.
    """
    delta = interval - time.time() % interval
    return delta + offset
