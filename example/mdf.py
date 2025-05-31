from framework.mdf import MDF

# load 1h stride data
mdf = MDF(['BTC', 'ETH', 'BNB', 'XRP'], ['swap'], stride='1h', history=500, quote='USDT', exchange='binance')

# check framework/dataFields.py for available datafields

mdf.MA60_close  # 60 moving average of close price
mdf.returns  # returns
mdf.returns24  # 24 hour returns
mdf.usd24  # 24 hour trading value in usd
mdf.amt24  # 24 hour trading value in amt