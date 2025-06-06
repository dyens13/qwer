from framework.mdf import MDF

# load 1h stride data
stride = '1h'  # 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 1w, ... check binance web
history = 5000  #
mdf = MDF(['BTC', 'ETH', 'BNB', 'XRP'], ['swap'], stride=stride, history=history, quote='USDT', exchange='binance')


# check framework/dataFields.py for available datafields and make your own

mdf.MA60_close  # 60 moving average of close price
mdf.returns  # returns
mdf.returns24  # 24 hour returns
mdf.usd24  # 24 hour trading value in usd
mdf.amt24  # 24 hour trading value in amt
vwap = mdf.usd24 / mdf.amt24  # volume weighted average price


mdf = MDF(['BTC', 'ETH', 'BNB', 'XRP'], ['spot', 'swap'], stride='1h', history=500, quote='USDT', exchange='binance')
# load data spot, swap both
#

spot_price = mdf.xclose
futures_price = mdf.close
spread = (mdf.close - mdf.xclose) / mdf.xclose

spot_returns = mdf.xreturns
futures_returns = mdf.returns
