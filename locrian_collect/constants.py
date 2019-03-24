"""
Constants used in locrian_collect
"""
from enum import Enum


class Side(Enum):
    """Enum of the trade side; either ask or bid."""
    asks = ask = 1
    bids = bid = 2


ORDER_MAP = {Side.ask: -1, Side.bid: 1}

BASE_DATA_DIRECTORY = '/home/chris/locrian/data'
NANOSECOND_FACTOR = 1000000000
MILLISECONDS_TO_NANOSECONDS = 1000000
CURRENCY_LIST = ('btc', 'bch', 'ltc', 'etc', 'eth')
CONTRACT_LIST = ('this_week', 'next_week', 'quarter')

OKCOIN = 'https://www.okcoin.com/api/v1/'
OKEX = 'https://www.okex.com/api/v1/'

BASE_URL_SPOT_TRADES = f'{OKCOIN}trades.do'
BASE_URL_SPOT_DEPTH = f'{OKCOIN}depth.do'

BASE_URL_FUTURE_TRADES = f'{OKEX}future_trades.do'
BASE_URL_INDEX = f'{OKEX}future_index.do'
BASE_URL_FUTURE_DEPTH = f'{OKEX}future_depth.do'
