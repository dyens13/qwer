import time
from datetime import datetime, timedelta

import requests

from data_collect.request_base import requests_get
from utils.ftns_datetime import convert_to_timestamp, dt_from_timestamp, interval_to_minute, convert_str_to_dt
from utils.ftns_general import list_str_to_float

exchange = 'upbit'
URLs = {
    'min': 'https://api.upbit.com/v1/candles/minutes',
    'day': 'https://api.upbit.com/v1/candles/days',
    'week': 'https://api.upbit.com/v1/candles/weeks',
    'month': 'https://api.upbit.com/v1/candles/months'
}

available_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']


def to_symbol(coin, quote):
    return f'{quote.upper()}-{coin.upper()}'


def get_available_coins(market, quote='KRW'):
    if market.upper() not in ['SPOT']:
        raise Exception('market must be spot or swap')
    result = requests.get('https://api.upbit.com/v1/market/all')
    js = result.json()
    coin_list = [x['market'].split('-')[1] for x in js if x['market'].startswith(quote.upper())]
    return coin_list


def get_klines(coin, quote, market, interval, endTime=None, length=None):
    # limit for spot : max 20022
    # ex_url : https://api.upbit.com/v1/candles/days?market=KRW-BTC&to=2023-04-01T01:00:00&count=100
    # ex_url : https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC&count=100
    if market.upper() not in ['SPOT']:
        raise Exception('market must be spot')
    if interval not in available_intervals:
        raise Exception(f'interval must be one of {available_intervals}')
    params = {'market': to_symbol(coin, quote)}
    if length:
        params['count'] = length
    if endTime:
        # output's last data's endtime > input endtime
        params['to'] = endTime
    # print(params)
    # result = requests.get(f'{URLs[market]}/klines', params=params)
    if interval in ['1m', '3m', '5m', '15m', '30m', '1h', '4h']:
        params['unit'] = interval_to_minute(interval)
        url = URLs['min'] + f'/{params["unit"]}'
    elif interval in ['1d']:
        url = URLs['day']
    elif interval in ['1w']:
        url = URLs['week']
    elif interval in ['1m']:
        url = URLs['month']
    else:
        raise Exception(f'interval must be one of {available_intervals}')
    result = requests_get(url, params=params)
    1+1
    try:
        call_limit = dict(item.split("=") for item in result.headers['Remaining-Req'].split("; "))
        # {'group': 'candles', ' min': '597', ' sec': '9'}
    except Exception as e:
        time.sleep(10)
        print('='*100)
        print(e)
        print('='*100)
        return get_klines(coin, quote, market, interval, endTime, length)
    else:
        if int(call_limit['min']) < 5:
            print(f'# sleep to avoid IP ban from high API CALL LIMIT\n# call limit for min : {int(call_limit["min"])}')
            time.sleep(5)
        elif int(call_limit['sec']) < 2:
            print(f'# sleep to avoid IP ban from high API CALL LIMIT\n# call limit for sec : {int(call_limit["sec"])}')
            time.sleep(1)
        raw_candles = result.json()
        output = []
        for candle in raw_candles[::-1]:
            output.append([datetime.timestamp(convert_str_to_dt(candle['candle_date_time_kst'])) * 1000,
                           candle['opening_price'], candle['high_price'], candle['low_price'], candle['trade_price'],
                           candle['candle_acc_trade_volume'], None, candle['candle_acc_trade_price'], None, None, None,
                           candle['candle_date_time_utc']])
            # candle['timestamp'] : last trade timestamp
        return output


def get_recent_klines(coin, quote, market, interval, length, only_complete=False, callfast=False):
    unit_limit = 200
    # print(coin, quote, market, interval)
    output = get_klines(coin, quote, market, interval, length=min(length, unit_limit))
    last_candle_time = dt_from_timestamp(output[-1][0])
    # fixme
    now = datetime.now() + timedelta(hours=9) - (datetime.now() - datetime.utcnow())
    print(f'-- {exchange} : {coin}{quote} {market} last candle start from now : {(now - last_candle_time).seconds / 60} mins')
    if (now - last_candle_time).seconds / 60 > interval_to_minute(interval):
        if coin == 'BTC':
            print('no recent candle')
            time.sleep(0.1)
            return get_recent_klines(coin, quote, market, interval, length, only_complete, callfast)
    if only_complete:
        now_m = now.minute
        if (now_m == last_candle_time.minute) or (now_m % interval_to_minute(interval) != 0):
            output = output[:-1]
    while length - unit_limit > 0:
        length -= unit_limit
        output = get_klines(coin, quote, market, interval, endTime=output[0][-1],
                            length=min(length, unit_limit)) + output
    return list_str_to_float(output)


if __name__ == '__main__':
    test = get_recent_klines('BTC', 'KRW', 'SPOT', '1d', length=300)
