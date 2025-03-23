import requests
from requests.exceptions import ConnectTimeout, ConnectionError
import time


def requests_get(url, num_retry=10, **url_params):
    if num_retry is None:
        num_retry = -1
    _log = f"- url : {url}\n- params : {url_params}"
    while int(num_retry) != 0:
        try:
            res = requests.get(url, **url_params)
            if str(res.status_code).startswith('2'):  # 200 : OK, 404 : NOT FOUND
                return res
            else:
                raise ConnectTimeout(f'[{res.status_code}] : {res.text}')

        except Exception as err:
            if isinstance(err, ConnectTimeout):
                print(f"!! ConnectTimeout !!\n{_log}")
                num_retry -= 1
                time.sleep(0.5)
                continue
            if isinstance(err, ConnectionError):
                print(f"!! ConnectionError !!\n{_log}")
                num_retry -= 1
                time.sleep(0.5)
                continue

            print(f'requests_get error : {err}')
            return None
    return None


if __name__ == '__main__':
    params = {
        'symbol': 'BTCUSDT',
        'interval': '1m',
        'limit': 5
    }
    test = requests_get(url='https://fapi.binance.com/fapi/v1/klines', params=params)
