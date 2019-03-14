import time
from threading import Thread

from locrian_collect.data_managers import get_managers
from .locrian_collect_logging import logger_order_book


def schedule_get_order_book():
    time_between_requests = 10  # seconds

    db_manager_list = get_managers()
    list_length = len(db_manager_list)

    if list_length/time_between_requests > 10:
        raise ValueError("Number of requests per second exceeds Okcoin's "
                         "limits (1 request every 0.1 seconds)")

    db_manager_list = get_managers()

    while True:
        time.sleep(delta_time_to_sleep(interval=time_between_requests))
        logger_order_book.info(f'Requesting order book.')
        thread_list = []
        for i in range(list_length):
            thread_list.append(Thread(target=db_manager_list[i].get_data))
        for i in range(list_length):
            thread_list[i].start()
        for i in range(list_length):
            thread_list[i].join()
        del thread_list


def delta_time_to_sleep(interval=10):
    delta = interval - time.time() % interval
    return delta
