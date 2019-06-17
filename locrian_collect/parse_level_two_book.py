import pandas as pd

from locrian_collect.constants import ORDER_MAP, Side


def parse_level_two_book(timestamp, book):
    """Parse level two book from json returned by exchange to level two dataframe.

    Parameters
    ----------
    timestamp: int
        The unix time in nanoseconds the request was made.
    book: dict
        The level two book as a dict; {'side': [price, volume]}

    Returns
    -------
    pd.DataFame
        Level two book as a pandas dataframe.
    """
    data = []
    for side in book:
        try:
            ordering = ORDER_MAP[Side[side]]
        except KeyError as key_error:
            print(side, book, timestamp)
            raise KeyError(key_error)

        levels = book[side][::ordering]
        side_as_value = Side[side].value

        for level_index, level in enumerate(levels, 1):
            price = level[0]
            volume = level[1]
            data.append([timestamp, side_as_value, level_index, price, volume])

    return pd.DataFrame(data, columns=['timestamp', 'side', 'level', 'price', 'volume'])
