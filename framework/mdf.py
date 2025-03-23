import warnings

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

from config.constants import TZ_shift
from data_collect.klines_mgr import get_recent_klines
from framework.dataFields import DerivedFields, WindowedFields, TSTransforms
from utils.ftns_datetime import interval_to_minute
from utils.ftns_general import split_str_num, dotdict

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

BASEFIELDS = {'open', 'high', 'low', 'close', 'amt', 'usd', 'vwap', 'trades', 'takerAmt', 'takerUsd'}


def klines_to_df(klines, exchange, source='api', scale=1):
    MAX_PRICE_RATIO = 5.0
    MIN_PRICE_RATIO = 0.2
    if source == 'api':
        df = pd.DataFrame(data=klines,
                          columns=['dt', 'open', 'high', 'low', 'close', 'amt', 'dt_close', 'usd', 'trades', 'takerAmt',
                                   'takerUsd', 'ignore'])
        if not type(df['dt']) is str:
            # df['dt'] = pd.to_datetime((df['dt'] + 1000 * 60 * 60 * TZ_shift) * 1000000, format='%Y-%m-%d %H:%M')
            df['dt'] = pd.to_datetime(df['dt'] + 1000 * 60 * 60 * TZ_shift, unit='ms')
        df = df.set_index('dt')
        # df['returns'] = (df.close / df.close.shift(1)).clip(MIN_PRICE_RATIO, MAX_PRICE_RATIO) - 1.0
        df['vwap'] = df['usd'] / df['amt']
        df['takerRatio'] = df['takerUsd'] / df['usd']
        del df['ignore']
        del df['dt_close']
    elif source == 'db':
        df = pd.DataFrame(data=klines,
                          columns=['start', 'open', 'high', 'low', 'close', 'cvolume', 'qvolume', 'trades',
                                   'takercvolume', 'takerqvolume'])
        df.columns = ['dt', 'open', 'high', 'low', 'close', 'amt', 'usd', 'trades', 'takerAmt', 'takerUsd']
        df = df.set_index('dt')
        # df['returns'] = (df.close / df.close.shift(1)).clip(MIN_PRICE_RATIO, MAX_PRICE_RATIO) - 1.0
        df['vwap'] = df['usd'] / df['amt']
        df['takerRatio'] = df['takerUsd'] / df['usd']
    else:
        raise ValueError(f'source must be api or db, not {source}')
    if scale > 1:
        for field in ['open', 'high', 'low', 'close', 'vwap']:
            df[field] *= 1000
        for field in ['amt', 'takerAmt']:
            df[field] /= 1000
    return df


def get_df(coin, quote, market, stride, history, exchange='binance', callfast=True):
    scale = 1
    if (coin.startswith('1000')) and (market.upper() == 'SPOT'):
        scale = 1000
        coin = coin.split('1000')[1]
    klines = get_recent_klines(coin, quote, market, stride, history, exchange, only_complete=True, callfast=callfast)
    return klines_to_df(klines, exchange, scale=scale)


class MDF:
    def __init__(self, coins, markets, stride, history, quote='USDT', exchange='binance', callfast=True):
        self._base_fields = set()
        self._loaded_fields = set()
        self.df = {}
        for coin in coins:
            if len(coin.split(quote)) > 1:
                coin = coin.split(quote)[0]
            if len(markets) == 1:
                smdf = get_df(coin, quote, markets[0], stride, history, exchange, callfast)
                fmdf = smdf
            else:
                smdf = get_df(coin, quote, 'spot', stride, history, exchange, callfast)
                fmdf = get_df(coin, quote, 'swap', stride, history, exchange, callfast)
            for field in BASEFIELDS:
                _new_field = False
                if field not in self.df.keys():
                    _new_field = True
                    self.df[field] = pd.DataFrame()
                    self._base_fields.add(field)
                    self._loaded_fields.add(field)
                self.df[field][coin] = eval(f'fmdf.{field}')
                if len(markets) == 2:
                    alt_field = 'x' + field
                    if _new_field:
                        self.df[alt_field] = pd.DataFrame()
                        self._base_fields.add(alt_field)
                        self._loaded_fields.add(alt_field)
                    self.df[alt_field][coin] = eval(f'smdf.{field}')
                """
                alt_field = 'x' + field
                if field not in self.df.keys():
                    self.df[alt_field] = pd.DataFrame()
                    self.df[field] = pd.DataFrame()
                    self._base_fields.add(field)
                    self._base_fields.add(alt_field)
                    self._loaded_fields.add(field)
                    self._loaded_fields.add(alt_field)
                self.df[alt_field][coin] = eval(f'smdf.{field}')
                self.df[field][coin] = eval(f'fmdf.{field}')
                """
        self.df = dotdict(self.df)
        self.nans = self.close.copy()
        self.nans[:] = np.nan
        self.zeros = self.close.copy()
        self.zeros[:] = 0

        self.stride = interval_to_minute(stride)
        self.day = int(1440 / self.stride)
        self.week = 7 * self.day
        self.month = 30 * self.day
        self.year = 365 * self.day
        self.hour = int(60 / self.stride) if self.stride <= 60 else None

        self.coins = list(self.df.close.columns)

    def __getattr__(self, field):
        if field in self._loaded_fields:
            return eval(f'self.df.{field}')
        else:
            if '_diff' in field:
                s, n = field.split('_diff')
                return getattr(self, s).diff(int(n))
            elif '_shift' in field:
                s, n = field.split('_shift')
                return getattr(self, s).shift(int(n))
            else:
                if field.startswith('swap'):
                    # fixme
                    raise NotImplementedError
                if '_' in field:
                    # prefix, body = field.split('_')
                    prefix = field.split('_')[0]
                    # body = field.split(prefix + '_')[1]
                    body = field[len(prefix) + 1:]
                    s, n = split_str_num(prefix)
                    temp_data = getattr(self, body)
                    data = getattr(TSTransforms, s)(temp_data, n)
                else:
                    s, n = split_str_num(field)
                    if n is None:
                        data = getattr(DerivedFields, s)(self)
                    else:
                        data = getattr(WindowedFields, s)(self, n)
                self.df[field] = data
                self._loaded_fields.add(field)
                return getattr(self, field)

    def clear_fields(self):
        _to_clear = self._loaded_fields - self._base_fields
        print(f'{_to_clear} removed')
        for field in _to_clear:
            if 'lw' in field:
                continue
            self.df.pop(field)
            self._loaded_fields.remove(field)
        import gc
        gc.collect()
        return None

    def set_prev_le(self, n, degree=1):
        prev_lwmax = self.nans.copy()
        prev_lwmin = self.nans.copy()
        dist_prev_lwmax = self.nans.copy()
        dist_prev_lwmin = self.nans.copy()

        for coin in self.coins:
            ##############################################
            # set prev local extremes
            ##############################################
            if degree < 0:
                raise ValueError('degree must be greater than 0!')
            cps = self.df.close.get(coin).values
            lwmax_index = argrelextrema(cps, np.greater_equal, order=n)[0]
            # lwmax_index = self._lwmax_index.get(n, argrelextrema(cps, np.greater_equal, order=n)[0])
            # self._lwmax_index[n] = lwmax_index
            for i, index in enumerate(lwmax_index):
                if i < degree:
                    continue
                prev_lwmax[coin][lwmax_index[i]] = self.close.get(coin)[lwmax_index[i - degree]]
            prev_lwmax[coin] = prev_lwmax[coin].fillna(method='ffill').shift(n)

            lwmin_index = argrelextrema(cps, np.less_equal, order=n)[0]
            # lwmin_index = self._lwmin_index.get(n, argrelextrema(cps, np.less_equal, order=n)[0])
            # self._lwmin_index[n] = lwmin_index
            for i, index in enumerate(lwmin_index):
                if i < degree:
                    continue
                prev_lwmin[coin][lwmin_index[i]] = self.close.get(coin)[lwmin_index[i - degree]]
            prev_lwmin[coin] = prev_lwmin[coin].fillna(method='ffill').shift(n)

            ##############################################
            # set distance to prev local extremes
            ##############################################
            recent_le_index = 0
            if len(lwmax_index) > 0:
                recent_le_di = lwmax_index[recent_le_index]
                for di in range(len(cps)):
                    if di < lwmax_index[recent_le_index + degree] + n:
                        continue
                    elif recent_le_index == len(lwmax_index) - 1 - degree:
                        pass
                    elif di >= lwmax_index[recent_le_index + degree + 1] + n:
                        recent_le_index += 1
                        recent_le_di = lwmax_index[recent_le_index]
                    dist_prev_lwmax[coin][di] = di - recent_le_di

            recent_le_index = 0
            if len(lwmin_index) > 0:
                recent_le_di = lwmin_index[recent_le_index]
                for di in range(len(cps)):
                    if di < lwmin_index[recent_le_index + degree] + n:
                        continue
                    elif recent_le_index == len(lwmin_index) - 1 - degree:
                        pass
                    elif di >= lwmin_index[recent_le_index + degree + 1] + n:
                        recent_le_index += 1
                        recent_le_di = lwmin_index[recent_le_index]
                    dist_prev_lwmin[coin][di] = di - recent_le_di
        setattr(self, f'prev{degree}_lwmax{n}', prev_lwmax)
        self._loaded_fields.add(f'prev{degree}_lwmax{n}')
        setattr(self, f'dist_prev{degree}_lwmax{n}', dist_prev_lwmax)
        self._loaded_fields.add(f'dist_prev{degree}_lwmax{n}')
        setattr(self, f'prev{degree}_lwmin{n}', prev_lwmin)
        self._loaded_fields.add(f'prev{degree}_lwmin{n}')
        setattr(self, f'dist_prev{degree}_lwmin{n}', dist_prev_lwmin)
        self._loaded_fields.add(f'dist_prev{degree}_lwmin{n}')

        return None

    def cps(self, coin, shift=0):
        return self.close.get(coin)[-(1 + shift)]

    def get(self, field):
        return eval(f'self.{field}')

    def get_field(self, coin, field, shift=0):
        output = 0
        try:
            output = self.get(field).get(coin)[-(1 + shift)]
        except Exception as e1:
            print(f'check {field} on get_field {coin}')
            print(e1)
        else:
            pass
        finally:
            return output


if __name__ == '__main__':
    # try mdf.close, mdf.vwap, mdf.tsZscore60_close, mdf.high60_tsZscore60_close, etc...
    mdf = MDF(['BTC', 'ETH', 'BNB', 'EOS'], ['swap'], stride='1h', history=500, quote='USDT', exchange='binance')
