import warnings

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

import data_collect.binance.klines_api as binance_klines_api
import data_collect.upbit.klines_api as upbit_klines_api
from config.constants import TZ_shift
from data_collect.klines_mgr import get_klines_range, get_recent_klines
from framework.dataFields import DerivedFields, TSTransforms, WindowedFields
from utils.ftns_datetime import interval_to_minute
from utils.ftns_general import dotdict, split_str_num

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

BASEFIELDS = {'open', 'high', 'low', 'close', 'amt', 'usd', 'vwap', 'trades', 'takerAmt', 'takerUsd'}
PRICE_FIELDS = {'open', 'high', 'low', 'close', 'vwap'}
SUPPORTED_MARKETS = {'spot', 'swap'}


def _normalize_market(market):
    market = market.lower()
    if market not in SUPPORTED_MARKETS:
        raise ValueError(f'market must be one of {sorted(SUPPORTED_MARKETS)}, not {market}')
    return market


def _normalize_coin(coin, quote):
    coin = coin.upper()
    if quote and coin.endswith(quote):
        return coin[:-len(quote)]
    return coin


def _get_available_coins(exchange, market, quote='USDT'):
    market = _normalize_market(market)
    exchange = exchange.lower()
    if exchange == 'binance':
        return binance_klines_api.get_available_coins(market, quote=quote)
    if exchange == 'upbit':
        return upbit_klines_api.get_available_coins(market, quote=quote)
    raise ValueError(f'Unsupported exchange: {exchange}')


def resolve_coin_name_and_scale(coin, market_list):
    def extract_scale_and_coin(symbol):
        import re

        match = re.match(r'^(\d{3,})([A-Z]+)$', symbol.upper())
        if match:
            return match.group(2), int(match.group(1))
        return symbol.upper(), 1

    base_coin, scale = extract_scale_and_coin(coin)

    if coin in market_list:
        return coin, 1
    if base_coin in market_list:
        return base_coin, scale

    for symbol in market_list:
        listed_coin, listed_scale = extract_scale_and_coin(symbol)
        if listed_coin == base_coin:
            return symbol, listed_scale

    raise ValueError(f'Cannot resolve coin name for {coin}')


def klines_to_df(klines, exchange, source='api', scale=1):
    max_price_ratio = 5.0
    min_price_ratio = 0.2
    if source == 'api':
        df = pd.DataFrame(
            data=klines,
            columns=[
                'dt',
                'open',
                'high',
                'low',
                'close',
                'amt',
                'dt_close',
                'usd',
                'trades',
                'takerAmt',
                'takerUsd',
                'ignore',
            ],
        )
        if not type(df['dt']) is str:
            df['dt'] = pd.to_datetime(df['dt'] + 1000 * 60 * 60 * TZ_shift, unit='ms')
        df = df.set_index('dt')
        df['vwap'] = df['usd'] / df['amt']
        df['takerRatio'] = df['takerUsd'] / df['usd']
        del df['ignore']
        del df['dt_close']
    elif source == 'db':
        df = pd.DataFrame(
            data=klines,
            columns=[
                'start',
                'open',
                'high',
                'low',
                'close',
                'cvolume',
                'qvolume',
                'trades',
                'takercvolume',
                'takerqvolume',
            ],
        )
        df.columns = ['dt', 'open', 'high', 'low', 'close', 'amt', 'usd', 'trades', 'takerAmt', 'takerUsd']
        df = df.set_index('dt')
        df['vwap'] = df['usd'] / df['amt']
        df['takerRatio'] = df['takerUsd'] / df['usd']
    else:
        raise ValueError(f'source must be api or db, not {source}')

    if scale > 1:
        for field in PRICE_FIELDS:
            df[field] *= scale
        for field in ['amt', 'takerAmt']:
            df[field] /= scale

    return df


def get_df(
    coin,
    quote,
    market,
    stride,
    history=None,
    exchange='binance',
    source='api',
    coin_list=None,
    callfast=True,
    start=None,
    end=None,
):
    if coin_list is None:
        coin_list = _get_available_coins(exchange, market, quote=quote)

    coin_resolved, scale = resolve_coin_name_and_scale(coin, coin_list)
    if source == 'api':
        if start is not None or end is not None:
            if exchange != 'binance':
                raise NotImplementedError('start/end range loading is only implemented for binance api source')
            if start is None:
                raise ValueError('start must be provided when using end')
            klines = get_klines_range(coin_resolved, quote, market, stride, start, end, exchange, callfast=callfast)
        else:
            klines = get_recent_klines(
                coin_resolved,
                quote,
                market,
                stride,
                history,
                exchange,
                only_complete=True,
                callfast=callfast,
            )
    elif source == 'db':
        raise NotImplementedError('db source is not included in this public project')
    else:
        raise ValueError(f'source must be api or db, not {source}')

    return klines_to_df(klines, exchange, source, scale=scale)


def _corr_btc_latest(obj, field='close', length=96, shift=0):
    field = str(field)
    shift = int(shift)
    cache = obj.__dict__.setdefault('_corr_btc_cache', {})

    window = obj._convert_timeframe(length)
    if window < 2:
        raise ValueError('length must be at least 2 bars for correlation')

    cache_key = (field, window, shift)
    if cache_key in cache:
        return cache[cache_key].copy()

    values = getattr(obj, field)
    if not isinstance(values, pd.DataFrame):
        raise TypeError(f"field '{field}' must resolve to a DataFrame, got {type(values)}")
    if 'BTC' not in values.columns:
        raise ValueError('BTC column is missing. BTC must be loaded to compute correlation.')

    if shift:
        values = values.shift(shift)

    latest = values.rolling(window, min_periods=window).corr(values['BTC']).iloc[-1].drop(labels=['BTC'], errors='ignore')
    cache[cache_key] = latest
    return latest.copy()


class MDF:
    def __init__(
        self,
        coins,
        markets,
        stride,
        history=None,
        quote='USDT',
        exchange='binance',
        source='api',
        callfast=True,
        start=None,
        end=None,
    ):
        if history is None and start is None:
            raise ValueError('Either history or start must be provided')

        self.quote = quote.upper()
        self.exchange = exchange.lower()
        self.source = source
        self.markets = [_normalize_market(market) for market in markets]

        self._base_fields = set()
        self._loaded_fields = set()
        self.df = {}
        self._market_views = {}
        self._quote_views = {}
        self._df_by_market = {market: {field: pd.DataFrame() for field in BASEFIELDS} for market in SUPPORTED_MARKETS}

        coins_norm = []
        for coin in coins:
            coins_norm.append(_normalize_coin(coin, self.quote))
        if 'BTC' not in coins_norm:
            coins_norm.append('BTC')

        market_coin_lists = {
            market: _get_available_coins(self.exchange, market, quote=self.quote)
            for market in set(self.markets)
        }

        self.missing_coins = []
        for coin in coins_norm:
            try:
                for market in self.markets:
                    resolve_coin_name_and_scale(coin, market_coin_lists[market])
            except ValueError:
                self.missing_coins.append(coin)
                continue

            frames = {}
            for market in self.markets:
                frames[market] = get_df(
                    coin,
                    self.quote,
                    market,
                    stride,
                    history=history,
                    exchange=self.exchange,
                    source=self.source,
                    coin_list=market_coin_lists[market],
                    callfast=callfast,
                    start=start,
                    end=end,
                )

            for field in BASEFIELDS:
                for market, frame in frames.items():
                    self._df_by_market[market][field][coin] = getattr(frame, field)

        self._default_market = 'swap' if 'swap' in self.markets else self.markets[0]
        self.df = dotdict(self._df_by_market[self._default_market])
        self._base_fields |= set(BASEFIELDS)
        self._loaded_fields |= set(BASEFIELDS)

        self.nans = self.close.copy()
        self.nans[:] = np.nan
        self.zeros = self.close.copy()
        self.zeros[:] = 0

        self.stride = interval_to_minute(stride)
        self.minute = 1 / self.stride
        self.day = int(1440 / self.stride)
        self.week = 7 * self.day
        self.month = 30 * self.day
        self.year = 365 * self.day
        self.hour = int(60 / self.stride) if 60 % self.stride == 0 else None

        self.coins = list(self.df.close.columns)

        if self.missing_coins:
            print(f'[MDF] Missing or unmatched coins: {self.missing_coins}')

    def __getattr__(self, field):
        if field.startswith('_'):
            raise AttributeError(field)

        if field in self._loaded_fields:
            return getattr(self.df, field)

        if '_diff' in field:
            base_field, diff_n = field.split('_diff', 1)
            return getattr(self, base_field).diff(int(diff_n))
        if '_shift' in field:
            base_field, shift_n = field.split('_shift', 1)
            return getattr(self, base_field).shift(int(shift_n))

        if '_' in field:
            prefix, body = field.split('_', 1)
            transform_name, transform_n = split_str_num(prefix)
            data = getattr(TSTransforms, transform_name)(getattr(self, body), transform_n)
        else:
            field_name, window = split_str_num(field)
            if window is None:
                data = getattr(DerivedFields, field_name)(self)
            else:
                data = getattr(WindowedFields, field_name)(self, window)

        self.df[field] = data
        self._loaded_fields.add(field)
        return data

    def m(self, market):
        market = _normalize_market(market)
        if self._df_by_market[market]['close'].empty:
            raise ValueError(f'{market} market not loaded: include {market!r} in markets')
        if market not in self._market_views:
            self._market_views[market] = _MarketView(self, market)
        return self._market_views[market]

    def q(self, quote):
        quote = quote.upper()
        if quote == self.quote:
            return self
        if quote != 'BTC':
            raise ValueError(f"currently supported quote views: '{self.quote}', 'BTC'")
        cache_key = (quote, self._default_market)
        if cache_key not in self._quote_views:
            self._quote_views[cache_key] = _QuoteView(self, quote=quote)
        return self._quote_views[cache_key]

    @property
    def btc(self):
        return self.q('BTC')

    def corr_btc(self, field='close', length=96, shift=0):
        return _corr_btc_latest(self, field=field, length=length, shift=shift)

    def clear_fields(self):
        fields_to_clear = self._loaded_fields - self._base_fields
        print(f'{fields_to_clear} removed')
        for field in list(fields_to_clear):
            if 'lw' in field:
                continue
            self.df.pop(field)
            self._loaded_fields.remove(field)
        if hasattr(self, '_corr_btc_cache'):
            self._corr_btc_cache.clear()
        import gc

        gc.collect()
        return None

    def set_prev_le(self, n, degree=1):
        prev_lwmax = self.nans.copy()
        prev_lwmin = self.nans.copy()
        dist_prev_lwmax = self.nans.copy()
        dist_prev_lwmin = self.nans.copy()

        for coin in self.coins:
            if degree < 0:
                raise ValueError('degree must be greater than 0!')

            close_values = self.close[coin].to_numpy()
            lwmax_index = argrelextrema(close_values, np.greater_equal, order=n)[0]
            lwmax_col = prev_lwmax.columns.get_loc(coin)
            for idx in range(degree, len(lwmax_index)):
                target = int(lwmax_index[idx])
                source = int(lwmax_index[idx - degree])
                prev_lwmax.iat[target, lwmax_col] = close_values[source]
            prev_lwmax[coin] = prev_lwmax[coin].ffill().shift(n)

            lwmin_index = argrelextrema(close_values, np.less_equal, order=n)[0]
            lwmin_col = prev_lwmin.columns.get_loc(coin)
            for idx in range(degree, len(lwmin_index)):
                target = int(lwmin_index[idx])
                source = int(lwmin_index[idx - degree])
                prev_lwmin.iat[target, lwmin_col] = close_values[source]
            prev_lwmin[coin] = prev_lwmin[coin].ffill().shift(n)

            recent_le_index = 0
            if len(lwmax_index) > degree:
                recent_le_di = lwmax_index[recent_le_index]
                dist_col = dist_prev_lwmax.columns.get_loc(coin)
                for di in range(len(close_values)):
                    if di < lwmax_index[recent_le_index + degree] + n:
                        continue
                    if recent_le_index != len(lwmax_index) - 1 - degree and di >= lwmax_index[recent_le_index + degree + 1] + n:
                        recent_le_index += 1
                        recent_le_di = lwmax_index[recent_le_index]
                    dist_prev_lwmax.iat[int(di), dist_col] = int(di - recent_le_di)

            recent_le_index = 0
            if len(lwmin_index) > degree:
                recent_le_di = lwmin_index[recent_le_index]
                dist_col = dist_prev_lwmin.columns.get_loc(coin)
                for di in range(len(close_values)):
                    if di < lwmin_index[recent_le_index + degree] + n:
                        continue
                    if recent_le_index != len(lwmin_index) - 1 - degree and di >= lwmin_index[recent_le_index + degree + 1] + n:
                        recent_le_index += 1
                        recent_le_di = lwmin_index[recent_le_index]
                    dist_prev_lwmin.iat[int(di), dist_col] = int(di - recent_le_di)

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
        return self.close[coin].iloc[-(1 + shift)]

    def get(self, field):
        return getattr(self, field)

    def get_field(self, coin, field, shift=0):
        output = 0
        try:
            output = self.get(field).get(coin).iloc[-(1 + shift)].item()
        except Exception as exc:
            print(f'check {field} on get_field {coin}')
            print(exc)
        finally:
            return output

    def MA(self, coin, length, field='close', pf=None, shift=0):
        length = self._convert_timeframe(length)
        if pf:
            return self.get_field(coin, f'{pf}_MA{length}_{field}', shift)
        return self.get_field(coin, f'MA{length}_{field}', shift)

    def std(self, coin, length, field='close', pf=None, shift=0):
        length = self._convert_timeframe(length)
        if pf:
            return self.get_field(coin, f'{pf}_tsStd{length}_{field}', shift)
        return self.get_field(coin, f'tsStd{length}_{field}', shift)

    def zsc(self, coin, length, field='close', pf=None, shift=0):
        length = self._convert_timeframe(length)
        if pf:
            return self.get_field(coin, f'{pf}_tsZscore{length}_{field}', shift)
        return self.get_field(coin, f'tsZscore{length}_{field}', shift)

    def _convert_timeframe(self, timeframe):
        if isinstance(timeframe, (int, float)):
            return int(timeframe)
        if not isinstance(timeframe, str):
            raise ValueError(f'timeframe must be string or number, not {type(timeframe)}')
        if len(timeframe) < 2:
            raise ValueError(f'Invalid timeframe: {timeframe}')

        number = int(timeframe[:-1])
        unit = timeframe[-1]
        if unit == 'm':
            total_minutes = number
        elif unit == 'h':
            total_minutes = number * 60
        elif unit == 'd':
            total_minutes = number * 1440
        elif unit == 'w':
            total_minutes = number * 7 * 1440
        elif unit == 'M':
            total_minutes = number * 30 * 1440
        elif unit == 'y':
            total_minutes = number * 365 * 1440
        else:
            raise ValueError(f'Invalid timeframe unit: {unit}. Use m, h, d, w, M, y')

        if total_minutes % self.stride != 0:
            raise ValueError(f'timeframe {timeframe} is not aligned with stride {self.stride}m')
        return total_minutes // self.stride


class _BaseView:
    def __init__(self, parent):
        self._parent = parent
        self._base_fields = set(BASEFIELDS)
        self._loaded_fields = set()
        self.df = dotdict({})

        self.stride = parent.stride
        self.minute = getattr(parent, 'minute', None)
        self.hour = parent.hour
        self.day = parent.day
        self.week = parent.week
        self.month = parent.month
        self.year = parent.year
        self.quote = parent.quote
        self.coins = parent.coins

        self.nans = parent.nans.copy()
        self.zeros = parent.zeros.copy()

        self._quote_views = {}

    def _root(self):
        current = self
        while hasattr(current, '_parent'):
            current = current._parent
        return current

    def _get_base(self, field):
        raise NotImplementedError

    def _convert_timeframe(self, timeframe):
        return self._root()._convert_timeframe(timeframe)

    def m(self, market):
        market_view = self._root().m(market)
        if self.quote == self._root().quote:
            return market_view
        return market_view.q(self.quote)

    def q(self, quote):
        quote = quote.upper()
        if quote == self.quote:
            return self
        if quote == self._root().quote:
            return self._parent
        if quote != 'BTC':
            raise ValueError(f"currently supported quote views: '{self._root().quote}', 'BTC'")
        if quote not in self._quote_views:
            self._quote_views[quote] = _QuoteView(self, quote=quote)
        return self._quote_views[quote]

    @property
    def btc(self):
        return self.q('BTC')

    def corr_btc(self, field='close', length=96, shift=0):
        return _corr_btc_latest(self, field=field, length=length, shift=shift)

    def __getattr__(self, field):
        if field in self._loaded_fields:
            return self.df[field]

        if field in BASEFIELDS:
            data = self._get_base(field)
            self.df[field] = data
            self._loaded_fields.add(field)
            return data

        if '_diff' in field:
            base_field, diff_n = field.split('_diff', 1)
            return getattr(self, base_field).diff(int(diff_n))
        if '_shift' in field:
            base_field, shift_n = field.split('_shift', 1)
            return getattr(self, base_field).shift(int(shift_n))

        if '_' in field:
            prefix, body = field.split('_', 1)
            transform_name, transform_n = split_str_num(prefix)
            data = getattr(TSTransforms, transform_name)(getattr(self, body), transform_n)
        else:
            field_name, window = split_str_num(field)
            if window is None:
                data = getattr(DerivedFields, field_name)(self)
            else:
                data = getattr(WindowedFields, field_name)(self, window)

        self.df[field] = data
        self._loaded_fields.add(field)
        return data

    def get(self, field):
        return getattr(self, field)

    def get_field(self, coin, field, shift=0):
        output = 0
        try:
            output = self.get(field).get(coin).iloc[-(1 + shift)].item()
        except Exception as exc:
            print(f'check {field} on get_field {coin}')
            print(exc)
        finally:
            return output

    def MA(self, coin, length, field='close', pf=None, shift=0):
        length = self._convert_timeframe(length)
        if pf:
            return self.get_field(coin, f'{pf}_MA{length}_{field}', shift)
        return self.get_field(coin, f'MA{length}_{field}', shift)

    def std(self, coin, length, field='close', pf=None, shift=0):
        length = self._convert_timeframe(length)
        if pf:
            return self.get_field(coin, f'{pf}_tsStd{length}_{field}', shift)
        return self.get_field(coin, f'tsStd{length}_{field}', shift)

    def zsc(self, coin, length, field='close', pf=None, shift=0):
        length = self._convert_timeframe(length)
        if pf:
            return self.get_field(coin, f'{pf}_tsZscore{length}_{field}', shift)
        return self.get_field(coin, f'tsZscore{length}_{field}', shift)


class _MarketView(_BaseView):
    def __init__(self, parent_mdf, market):
        super().__init__(parent_mdf)
        self.market = _normalize_market(market)

    def _get_base(self, field):
        try:
            return self._parent._df_by_market[self.market][field]
        except KeyError as exc:
            raise AttributeError(f'Missing market/base field: market={self.market}, field={field}') from exc


class _QuoteView(_BaseView):
    def __init__(self, parent_view_or_mdf, quote='BTC'):
        super().__init__(parent_view_or_mdf)
        self.quote = quote.upper()
        if self.quote != 'BTC':
            raise ValueError('only BTC quote is supported for now')
        self._px_cache = {}

    def _get_base(self, field):
        base = getattr(self._parent, field)
        if field not in PRICE_FIELDS:
            return base
        if field in self._px_cache:
            return self._px_cache[field]
        if 'BTC' not in base.columns:
            raise ValueError(f"BTC column is missing in parent base field '{field}'. BTC must be loaded.")

        output = base.div(base['BTC'], axis=0)
        output['BTC'] = 1.0
        self._px_cache[field] = output
        return output
