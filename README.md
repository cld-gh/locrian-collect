# Locrian Collect

### Install and Setup

Create a new environment.
```
conda create --name locrian-collect python=3.6
```

Install requirements
```
pip install -r requirements.txt
```

Install package by
```
pip install -e .
```

For development and running tests
```
pip install -r requirements-dev.txt
```

### Database Management
Data is stored in three databases named `locrian_level_two`, `locrian_trades`, and `locrian_future_index`.
See below for names and schemas of the tables.

#### Level Two books (locrian_level_two)

`locrian_level_two` stores the level two order books for spot and futures with the following table naming 
convention:

`future_{crypto_currency}_{contract_expiry}`\
`spot_{crypto_currency}`

where `crypto_currency` is either btc, bch, etc, eth, or ltc, and `contract_expirty` is either this_week,
next_week, or quarter.

Schema:\
timestamp - bitint(20)\
side - tinyint(4)\
level - smallint(6)\
price - double\
volume - double


#### Trades data (locrian_trades)
Similarly to the data store of level two order books, the trades data is stored in tables with the names:

`trades_future_{contract_expiry}_{crypto_currency}`\
`trades_spot_{crypto_currency}`

Schema:\
unixRequestTime - bitint(20)\
unixReturnTime - bitint(20)\
trade_time - bitint(20)\
amount - float\
price - float\
side - varchar(4)\
tid - bigint(20)


#### Future Index (locrian_trades)
The future index, is an index constructed from the prices of different exchanges and is a time-weighted average
over the past hour.  This index is computed by OkCoin and is used as the reference price for settling contracts.

`future_index_{crypto_currency}`

Schema:\
unixRequestTime - bitint(20)\
unixReturnTime - bitint(20)\
future_index - float