"""
Schedule the collection of data.
"""
import time
from threading import Thread

from .data_managers import get_trades_managers, get_managers
from .logs import logger_trades, logger_order_book

PARAMS_MAP = {'trades': {'managers': get_trades_managers(),
                         'time_between_requests': 100,  # seconds
                         'logger': logger_trades,
                         'log_msg': 'Requesting trades.',
                         'offset': 0.1
                         },
              'order_book': {'managers': get_managers(),
                             'time_between_requests': 10,  # seconds
                             'logger': logger_order_book,
                             'log_msg': 'Requesting order book and futures index.',
                             'offset': 0.001
                             }}


def scheduler(data_to_record):
    """Schedule the recording of data (order book, index or trades).

    Parameters
    ----------
    data_to_record: str
        The type of data to record, either 'order_book' or 'trades'

    """
    params = PARAMS_MAP[data_to_record]

    db_managers = params['managers']
    time_between_requests = params['time_between_requests']
    logger = params['logger']
    log_msg = params['log_msg']
    offset = params['offset']

    num_managers = len(db_managers)

    if num_managers/time_between_requests > 10:
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
    """Time between now and the next timestamp where the
    timestamp modulo interval is zero

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
