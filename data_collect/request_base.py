import re
import time
from datetime import datetime

import requests
from requests.exceptions import ConnectTimeout, ConnectionError, HTTPError, ReadTimeout, RequestException


def requests_get(url, num_retry=10, timeout=(3.05, 20), **url_params):
    if num_retry is None:
        num_retry = -1
    _log = f"- url : {url}\n- params : {url_params}"
    while int(num_retry) != 0:
        try:
            if 'timeout' not in url_params:
                url_params['timeout'] = timeout
            res = requests.get(url, **url_params)
            if str(res.status_code).startswith('2'):  # 200 : OK, 404 : NOT FOUND
                return res
            else:
                body_preview = res.text[:300].replace('\n', ' ')
                raise HTTPError(f'[{res.status_code}] : {body_preview}', response=res)

        except Exception as err:
            if isinstance(err, ConnectTimeout):
                print(f"!! ConnectTimeout !! {err}\n{_log}")
                num_retry -= 1
                time.sleep(0.5)
                continue
            if isinstance(err, ReadTimeout):
                print(f"!! ReadTimeout !! {err}\n{_log}")
                num_retry -= 1
                time.sleep(0.5)
                continue
            if isinstance(err, ConnectionError):
                print(f"!! ConnectionError !! {err}\n{_log}")
                num_retry -= 1
                time.sleep(0.5)
                continue
            if isinstance(err, HTTPError):
                response = getattr(err, 'response', None)
                status_code = response.status_code if response is not None else None
                print(f"!! HTTPError !! {err}\n{_log}")

                if status_code == 418:
                    text = response.text if response is not None else str(err)
                    match = re.search(r'banned until (\d{13})', text)
                    if match:
                        banned_until_ms = int(match.group(1))
                        wait_seconds = max(0.0, banned_until_ms / 1000 - time.time()) + 1.0
                        banned_until_dt = datetime.fromtimestamp(banned_until_ms / 1000)
                        print(f'# 418 ban detected; stop retry until {banned_until_dt} (sleep {wait_seconds:.1f}s)')
                        time.sleep(wait_seconds)
                    return None

                if status_code == 429:
                    retry_after = 0.0
                    if response is not None:
                        retry_after_header = response.headers.get('Retry-After')
                        if retry_after_header:
                            try:
                                retry_after = float(retry_after_header)
                            except ValueError:
                                retry_after = 0.0
                    wait_seconds = max(1.0, retry_after or 5.0)
                    num_retry -= 1
                    print(f'# 429 rate limit detected; retry after {wait_seconds:.1f}s')
                    time.sleep(wait_seconds)
                    continue

                num_retry -= 1
                time.sleep(0.5)
                continue
            if isinstance(err, RequestException):
                print(f"!! RequestException !! {type(err).__name__}: {err}\n{_log}")
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
