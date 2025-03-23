import os
from pathlib import Path

_CUR_DIR = Path(os.path.dirname(__file__))
PARENT_DIR = str(_CUR_DIR.parent)
api_yaml = f'{PARENT_DIR}/config/api_key.yaml'

TZ_shift = 9

PERIOD_DICT = {1: '1m', 5: '5m', 15: '15m', 30: '30m', 60: '1h', 240: '4h', 1440: '1d', 10080: '1w'}

withdrawl_fees = {
    'binance': {
        'BTC': 0.0005
    },
    'upbit': {
        'BTC': 0.0009,
        'ETH': 0.018,
        'XRP': 1,
        'TRX': 1,
        'WAVES': 0.001
    }
}

trading_fees = {
    'binance': 0.01 * 0.0360,
    'upbit': 0.01 * 0.5
}

simBK = 200
