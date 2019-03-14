import time
from threading import Thread

from locrian_collect.data_managers import get_trades_managers, get_managers
from .locrian_collect_logging import logger_trades, logger_order_book

PARAMS_MAP = {'trades': {'managers': get_trades_managers(),
                         'time_between_requests': 100,  # seconds
                         'logger': logger_trades,
                         'log_msg': 'Requesting trades.',
                         },
              'order_book': {'managers': get_managers(),
                             'time_between_requests': 10,  # seconds
                             'logger': logger_order_book,
                             'log_msg': 'Requesting order book and futures index.'
                             }}


def scheduler(data_to_record):
    params = PARAMS_MAP[data_to_record]

    db_managers = params['managers']
    time_between_requests = params['time_between_requests']
    logger = params['logger']
    log_msg = params['log_msg']

    num_managers = len(db_managers)

    if num_managers/time_between_requests > 10:
        raise ValueError("Number of requests per second exceeds Okcoin's "
                         "limits (1 request every 0.1 seconds)")

    while True:
        time.sleep(delta_time_to_sleep(interval=time_between_requests))
        logger.info(log_msg)
        thread_list = []
        for database_manager in db_managers:
            thread_list.append(Thread(target=database_manager.get_data))
        for i in range(num_managers):
            thread_list[i].start()
        for i in range(num_managers):
            thread_list[i].join()
        del thread_list


def delta_time_to_sleep(interval=10):
    delta = interval - time.time() % interval
    return delta + 0.1

