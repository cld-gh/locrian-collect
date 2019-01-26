import time
from threading import Thread

from .constants import BASE_URL_FUTURE, BASE_URL_SPOT
from squirrel.constants import CURRENCY_LIST, CONTRACT_LIST
from .trades_manager import TradesManager
from ..squirrel_logging import logger_trades


def get_urls_and_mysql_tables():
    assets = []
    for currency in CURRENCY_LIST:
        assets.append({'mysql_table': f'trades_spot_{currency}',
                       'url': f'{BASE_URL_SPOT}?symbol={currency}_usd'})
        for contract in CONTRACT_LIST:
            assets.append({'mysql_table': f'trades_future_{contract}_{currency}',
                           'url': f'{BASE_URL_FUTURE}?symbol={currency}_usd&contract_type={contract}'})
    return assets


def get_mysql_objects():
    assets = get_urls_and_mysql_tables()

    trades_manager_list = []
    for asset in assets:
        trades_manager_list.append(TradesManager(**asset))

    return trades_manager_list


def schedule_get_trades():
    time_between_requests = 100  # seconds

    db_manager_list = get_mysql_objects()
    list_length = len(db_manager_list)

    if list_length/time_between_requests > 10:
        raise ValueError("Number of requests per second exceeds Okcoin's "
                         "limits (1 request every 0.1 seconds)")

    db_manager_list = get_mysql_objects()

    while True:
        time.sleep(delta_time_to_sleep(interval=time_between_requests))
        logger_trades.info(f'Requesting trades.')
        thread_list = []
        for i in range(list_length):
            thread_list.append(Thread(target=db_manager_list[i].get_trades))
        for i in range(list_length):
            thread_list[i].start()
        for i in range(list_length):
            thread_list[i].join()
        del thread_list


def delta_time_to_sleep(interval=10):
    delta = interval - time.time() % interval
    return delta
