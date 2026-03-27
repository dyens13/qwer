from framework.mdf import MDF

# -----------------------------------------------------------------------------
# 1) basic usage: load swap candles
# -----------------------------------------------------------------------------
stride = '1h'  # 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 1w, ...
history = 5000

mdf = MDF(['BTC', 'ETH', 'BNB', 'XRP'], ['swap'], stride=stride, history=history, quote='USDT', exchange='binance')

# check framework/dataFields.py for available data fields and make your own
mdf.MA60_close  # 60 moving average of close price
mdf.returns  # 1-bar returns
mdf.returns24  # 24-bar returns
mdf.usd24  # 24-bar trading value in usd
mdf.amt24  # 24-bar trading volume in coin units
vwap = mdf.usd24 / mdf.amt24  # volume weighted average price

mdf.MA('ETH', '4h')  # helper for scalar lookup
mdf.zsc('ETH', '1d')  # helper with timeframe string
corr = mdf.corr_btc(field='returns', length='2d')  # latest rolling corr vs BTC


# -----------------------------------------------------------------------------
# 2) load spot + swap together
# -----------------------------------------------------------------------------
mdf = MDF(['BTC', 'ETH', 'BNB', 'XRP'], ['spot', 'swap'], stride='1h', history=500, quote='USDT', exchange='binance')

# default market is swap
futures_price = mdf.close
futures_returns = mdf.returns

# spot/swap market view API
spot = mdf.m('spot')
swap = mdf.m('swap')

spot_price = spot.close
spot_returns = spot.returns
spread = (swap.close - spot.close) / spot.close


# -----------------------------------------------------------------------------
# 3) quote view: convert COIN/USDT -> COIN/BTC
# -----------------------------------------------------------------------------
btc_quote = mdf.q('BTC')
swap_close_btc = btc_quote.close  # swap COIN / BTC
spot_close_btc = mdf.m('spot').q('BTC').close  # spot COIN / BTC

# convenience alias
same_as_q = mdf.btc.close.equals(mdf.q('BTC').close)


# -----------------------------------------------------------------------------
# 4) optional range loading for binance api
# -----------------------------------------------------------------------------
mdf_range = MDF(
    ['BTC', 'ETH'],
    ['swap'],
    stride='1h',
    start='2024-01-01 00:00:00',
    end='2024-02-01 00:00:00',
    quote='USDT',
    exchange='binance',
    source='api',
)

range_ret = mdf_range.returns24
