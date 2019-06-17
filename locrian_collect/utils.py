from .data_managers import TradesManager, OrderBookManager, IndexManager
from .constants import (
    CURRENCY_LIST, BASE_URL_SPOT_TRADES, CONTRACT_LIST, BASE_URL_FUTURE_TRADES,
    BASE_URL_SPOT_DEPTH, BASE_URL_INDEX, BASE_URL_FUTURE_DEPTH
)


def trades_url_mysql_maps():
    """Return a list of dicts where the dicts have information for the mysql_table and url
    to get the data."""
    assets = []
    for currency in CURRENCY_LIST:
        assets.append({'mysql_table': f'trades_spot_{currency}',
                       'url': f'{BASE_URL_SPOT_TRADES}?symbol={currency}_usd'})
        for contract in CONTRACT_LIST:
            assets.append({
                'mysql_table':
                    f'trades_future_{contract}_{currency}',
                'url':
                    f'{BASE_URL_FUTURE_TRADES}?symbol={currency}_usd&contract_type={contract}'})
    return assets


def get_trades_managers():
    """Get a list of Trades Managers"""
    assets = trades_url_mysql_maps()

    trades_managers = []
    for asset in assets:
        trades_managers.append(TradesManager(**asset))

    return trades_managers


def get_managers():
    """Get a list of Managers for order books and future indexes."""
    managers = []
    for currency in CURRENCY_LIST:
        managers.append(OrderBookManager(asset_name=f'spot_{currency}',
                                         mysql_table=f'spot_{currency}_usd_orderbook',
                                         url=f'{BASE_URL_SPOT_DEPTH}?symbol={currency}_usd'))

        managers.append(IndexManager(mysql_table=f'future_index_{currency}_usd',
                                     url=f'{BASE_URL_INDEX}?symbol={currency}_usd'))

        for contract in CONTRACT_LIST:
            url = f'{BASE_URL_FUTURE_DEPTH}?symbol={currency}_usd&contract_type={contract}&size=200'
            managers.append(
                OrderBookManager(asset_name=f'future_{currency}_{contract}',
                                 mysql_table=f'future_{currency}_usd_{contract}_orderbook',
                                 url=url))

    return managers
