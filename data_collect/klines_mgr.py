import data_collect.binance.klines_api
import data_collect.upbit.klines_api


def get_recent_klines(coin, quote, market, interval, length, exchange, only_complete=False, callfast=False):
    if exchange == 'binance':
        return data_collect.binance.klines_api.get_recent_klines(coin, quote, market, interval, length, only_complete=only_complete, callfast=callfast)
    elif exchange == 'upbit':
        return data_collect.upbit.klines_api.get_recent_klines(coin, quote, market, interval, length, only_complete=only_complete, callfast=callfast)
