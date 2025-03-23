import time
from datetime import datetime

import requests

from data_collect.request_base import requests_get
from utils.ftns_datetime import convert_to_timestamp, dt_from_timestamp, interval_to_minute
from utils.ftns_general import list_str_to_float

exchange = 'binance'
spotURL = 'https://api.binance.com/api/v3'  # https://api.binance.com/api/v3/exchangeInfo
swapURL = 'https://fapi.binance.com/fapi/v1'  # https://fapi.binance.com/fapi/v1/exchangeInfo
URLs = {'SPOT': spotURL, 'SWAP': swapURL}

available_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']


def to_symbol(coin, quote):
    return f'{coin.upper()}{quote.upper()}'


def get_coin_list(quote='USDT'):
    spot_list = get_available_coins('spot', quote=quote)
    swap_list = get_available_coins('swap', quote=quote)
    return [x for x in spot_list if x in swap_list]


def get_available_coins(market, quote='USDT'):
    if market.upper() not in ['SPOT', 'SWAP']:
        raise Exception('market must be spot or swap')
    result = requests.get(f'{URLs[market.upper()]}/ticker/price')
    js = result.json()
    symbols = [x['symbol'] for x in js]
    coin_list = [x[:-1 * len(quote)] for x in symbols if x.endswith(quote.upper())]
    return coin_list


def get_klines(coin, quote, market, interval, startTime=None, endTime=None, length=None):
    # limit for spot : max 1000, default 500, weight 1
    # limit for swap : max 1500, default 500, weight 1(99), 2(499), 5(1000), 10(1500)
    # timestamp in milliseconds
    # ex_url : https://fapi.binance.com/fapi/v1/klines?interval=15m&symbol=BTCUSDT&limit=2
    # ex output : [1680912000000, '27906.34000000', '28154.99000000', '27859.02000000', '27936.61000000', '16888.70036000', 1680998399999, '472941949.37071220', 509776, '8449.28557000', '236615094.39698220', '0']
    if market.upper() not in ['SPOT', 'SWAP']:
        raise Exception('market must be spot or swap')
    if interval not in available_intervals:
        raise Exception(f'interval must be one of {available_intervals}')
    params = {'symbol': to_symbol(coin, quote), 'interval': interval}
    if length:
        params['limit'] = length
    if startTime:
        # output's first data's starttime >= input starttime
        params['startTime'] = convert_to_timestamp(startTime, 'milliseconds')
    if endTime:
        # output's last data's endtime > input endtime
        params['endTime'] = convert_to_timestamp(endTime, 'milliseconds')
    # print(params)
    # result = requests.get(f'{URLs[market]}/klines', params=params)
    result = requests_get(f'{URLs[market.upper()]}/klines', params=params)
    # print(f"# current api limit : {result.headers['X-MBX-USED-WEIGHT-1M']}")
    """
    if int(result.headers['X-MBX-USED-WEIGHT-1M']) > 2300:
        print('# sleep to avoid IP ban from high API CALL LIMIT')
        time.sleep(10)
    return result.json()
    """
    try:
        call_limit = int(result.headers['X-MBX-USED-WEIGHT-1M'])
    except Exception as e:
        time.sleep(10)
        print('='*100)
        print(e)
        print('='*100)
        return get_klines(coin, quote, market, interval, startTime, endTime, length)
    else:
        if call_limit > 2300:
            print('# sleep to avoid IP ban from high API CALL LIMIT')
            time.sleep(10)
        return result.json()


def get_recent_klines(coin, quote, market, interval, length, only_complete=False, callfast=True):
    # limit 2400/1m
    if market.lower() == 'spot':
        unit_limit = 1000
    elif market.lower() == 'swap':
        if callfast:
            unit_limit = 1500
        else:
            unit_limit = 499
    else:
        raise ValueError('inst should be one of (spot, swap)')

    output = get_klines(coin, quote, market, interval, length=min(length, unit_limit))
    last_candle_time = dt_from_timestamp(output[-1][0])
    now = datetime.now()
    print(f'-- {exchange} : {coin}{quote} {market} last candle start from now : {(now - last_candle_time).seconds / 60} mins')
    """
    if (now - last_candle_time).seconds / 60 > interval_to_minute(interval):
        print('no recent candle')
        time.sleep(0.1)
        return get_recent_klines(coin, quote, market, interval, length, only_complete, callfast)
    """
    if only_complete:
        now_m = now.minute
        if (now_m == last_candle_time.minute) or (now_m % interval_to_minute(interval) != 0):
            output = output[:-1]
    while length - unit_limit > 0:
        length -= unit_limit
        output = get_klines(coin, quote, market, interval, endTime=int(output[0][0] - 1), length=min(length, unit_limit)) + output
    return list_str_to_float(output)


if __name__ == '__main__':
    test = get_recent_klines('BTC', 'USDT', 'SPOT', '1m', length=2000)
