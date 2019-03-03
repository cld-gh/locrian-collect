import time
from threading import Thread

from ..constants import CONTRACT_LIST, CURRENCY_LIST
from .order_book_manager import OrderBookManager, IndexManager
from ..locrian_collect_logging import logger_order_book


def get_managers():
    managers = []
    for currency in CURRENCY_LIST:
        managers.append(OrderBookManager(mysql_table=f'spot_{currency}_usd_orderbook',
                                         url=f'https://www.okcoin.com/api/v1/depth.do?symbol={currency}_usd'))

        managers.append(IndexManager(mysql_table=f'future_index_{currency}_usd',
                                     url=f'https://www.okex.com/api/v1/future_index.do?symbol={currency}_usd'))

        for contract in CONTRACT_LIST:
            url = f'https://www.okex.com/api/v1/future_depth.do?symbol={currency}_usd&contract_type={contract}&size=50'
            managers.append(OrderBookManager(mysql_table=f'future_{currency}_usd_{contract}_orderbook', url=url))

    return managers


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
            thread_list.append(Thread(target=db_manager_list[i].get_order_book))
        for i in range(list_length):
            thread_list[i].start()
        for i in range(list_length):
            thread_list[i].join()
        del thread_list


def delta_time_to_sleep(interval=10):
    delta = interval - time.time() % interval
    return delta
