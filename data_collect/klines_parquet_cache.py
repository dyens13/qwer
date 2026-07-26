from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.constants import DATA_CACHE_DIR, TZ_shift
from data_collect.binance import klines_api as binance_klines_api
from data_collect.klines_mgr import get_klines_range, get_recent_klines
from data_collect.upbit import klines_api as upbit_klines_api
from utils.ftns_datetime import convert_str_to_dt, interval_to_minute

CACHE_COLUMNS = ['open', 'high', 'low', 'close', 'amt', 'usd', 'trades', 'takerAmt', 'takerUsd']
RECENT_REFRESH_OVERLAP_BARS = 3
OPEN_RANGE_REFRESH_OVERLAP_BARS = 3


def load_cached_klines_df(
    coin,
    quote,
    market,
    interval,
    exchange='binance',
    history=None,
    callfast=True,
    start=None,
    end=None,
):
    cache_path = get_cache_path(exchange, market, quote, interval, coin)
    cache_df = read_cache_df(cache_path)

    if start is not None or end is not None:
        if start is None:
            raise ValueError('start must be provided when using end')
        cache_df_to_store, output_df = _load_range_df(
            cache_df=cache_df,
            coin=coin,
            quote=quote,
            market=market,
            interval=interval,
            exchange=exchange,
            start=start,
            end=end,
            callfast=callfast,
        )
    else:
        if history is None:
            raise ValueError('history must be provided when start/end are not used')
        cache_df_to_store, output_df = _load_recent_df(
            cache_df=cache_df,
            coin=coin,
            quote=quote,
            market=market,
            interval=interval,
            exchange=exchange,
            history=history,
            callfast=callfast,
        )

    cache_df_to_store = trim_in_progress_cache_df(cache_df_to_store, interval)
    write_cache_df(cache_path, cache_df_to_store)
    return output_df


def get_cache_path(exchange, market, quote, interval, coin) -> Path:
    return DATA_CACHE_DIR / 'klines' / str(exchange).lower() / str(market).lower() / str(quote).upper() / str(interval) / f'{str(coin).upper()}.parquet'


def read_cache_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return empty_cache_df()
    df = pd.read_parquet(path)
    return normalize_cache_df(df, duplicate_context=f'cache file {path}')


def write_cache_df(path: Path, df: pd.DataFrame) -> None:
    normalized_df = normalize_cache_df(df, duplicate_context=f'cache write {path}')
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix('.tmp.parquet')
    normalized_df.to_parquet(tmp_path)
    tmp_path.replace(path)


def empty_cache_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=CACHE_COLUMNS)
    df.index = pd.DatetimeIndex([], name='dt')
    return df


def ensure_unique_index(df: pd.DataFrame, context: str) -> None:
    duplicate_mask = df.index.duplicated(keep=False)
    if not duplicate_mask.any():
        return
    duplicate_index = pd.Index(df.index[duplicate_mask]).unique()
    sample = ', '.join(str(x) for x in duplicate_index[:5])
    raise ValueError(f'Duplicate candle timestamps found in {context}: {sample}')


def normalize_cache_df(df: pd.DataFrame, allow_duplicate_index=False, duplicate_context='cache dataframe') -> pd.DataFrame:
    if df is None or len(df) == 0:
        return empty_cache_df()

    out = df.copy()
    if 'dt' in out.columns:
        out = out.set_index('dt')
    out.index = pd.to_datetime(out.index)
    out.index.name = 'dt'
    out = out.sort_index()
    if allow_duplicate_index:
        out = out[~out.index.duplicated(keep='last')]
    else:
        ensure_unique_index(out, duplicate_context)

    for col in CACHE_COLUMNS:
        if col not in out.columns:
            out[col] = float('nan')
    out[CACHE_COLUMNS] = out[CACHE_COLUMNS].apply(pd.to_numeric, errors='coerce')
    return out[CACHE_COLUMNS]


def api_klines_to_cache_df(klines) -> pd.DataFrame:
    if not klines:
        return empty_cache_df()

    df = pd.DataFrame(
        data=klines,
        columns=['dt', 'open', 'high', 'low', 'close', 'amt', 'dt_close', 'usd', 'trades', 'takerAmt', 'takerUsd', 'ignore'],
    )
    df['dt'] = pd.to_datetime(df['dt'] + 1000 * 60 * 60 * TZ_shift, unit='ms')
    df = df.set_index('dt')
    df.index.name = 'dt'
    df[CACHE_COLUMNS] = df[CACHE_COLUMNS].apply(pd.to_numeric, errors='coerce')
    return normalize_cache_df(df[CACHE_COLUMNS], duplicate_context='api klines batch')


def merge_cache_df(base_df: pd.DataFrame, add_df: pd.DataFrame) -> pd.DataFrame:
    base_df = normalize_cache_df(base_df, duplicate_context='existing cache before merge')
    add_df = normalize_cache_df(add_df, duplicate_context='new api data before merge')
    if len(base_df) == 0:
        return add_df
    if len(add_df) == 0:
        return base_df
    merged = pd.concat([base_df, add_df])
    return normalize_cache_df(merged, allow_duplicate_index=True, duplicate_context='cache merge overlap')


def _load_recent_df(cache_df, coin, quote, market, interval, exchange, history, callfast):
    bar_delta = _bar_delta(interval)
    latest_complete_dt = latest_complete_open_dt(interval)
    updated_df = normalize_cache_df(cache_df)

    if len(updated_df) > 0:
        updated_df = updated_df.loc[:latest_complete_dt]

    if len(updated_df) == 0:
        recent_df = fetch_recent_df(coin, quote, market, interval, history, exchange, callfast)
        updated_df = normalize_cache_df(recent_df)
        return updated_df, updated_df.tail(history)

    last_cached_dt = updated_df.index.max()
    if last_cached_dt < latest_complete_dt:
        missing_tail_bars = count_bars_between(last_cached_dt, latest_complete_dt, bar_delta)
        refresh_bars = max(missing_tail_bars + RECENT_REFRESH_OVERLAP_BARS, RECENT_REFRESH_OVERLAP_BARS + 1)
        recent_df = fetch_recent_df(coin, quote, market, interval, refresh_bars, exchange, callfast)
        updated_df = merge_cache_df(updated_df, recent_df)

    if len(updated_df) < history:
        missing_head_bars = history - len(updated_df)
        older_df = fetch_older_df(coin, quote, market, interval, exchange, updated_df.index.min(), missing_head_bars, callfast)
        updated_df = merge_cache_df(updated_df, older_df)

    updated_df = normalize_cache_df(updated_df)
    return updated_df, updated_df.tail(history)


def _load_range_df(cache_df, coin, quote, market, interval, exchange, start, end, callfast):
    if exchange != 'binance':
        raise NotImplementedError('start/end range loading is only implemented for binance parquet source')

    bar_delta = _bar_delta(interval)
    start_dt = align_range_start(start, interval)
    latest_complete_dt = latest_complete_open_dt(interval)
    if end is None:
        end_dt = latest_complete_dt
        refresh_tail = True
    else:
        end_dt = min(align_range_end(end, interval), latest_complete_dt)
        refresh_tail = False
    updated_df = normalize_cache_df(cache_df)

    if start_dt > end_dt:
        empty_df = empty_cache_df()
        return updated_df, empty_df

    if len(updated_df) == 0:
        fetched_df = fetch_range_df(coin, quote, market, interval, exchange, start_dt, end_dt, callfast)
        fetched_df = normalize_cache_df(fetched_df)
        return fetched_df, slice_range_df(fetched_df, start_dt, end_dt)

    first_cached_dt = updated_df.index.min()
    if start_dt < first_cached_dt:
        left_end_dt = first_cached_dt - bar_delta
        left_df = fetch_range_df(coin, quote, market, interval, exchange, start_dt, left_end_dt, callfast)
        updated_df = merge_cache_df(updated_df, left_df)

    requested_df = slice_range_df(updated_df, start_dt, end_dt)
    expected_index = pd.date_range(start=start_dt, end=end_dt, freq=bar_delta)
    missing_index = expected_index.difference(requested_df.index)

    if len(missing_index) > 0:
        for seg_start_dt, seg_end_dt in split_missing_segments(missing_index, bar_delta):
            seg_df = fetch_range_df(coin, quote, market, interval, exchange, seg_start_dt, seg_end_dt, callfast)
            updated_df = merge_cache_df(updated_df, seg_df)

    if refresh_tail:
        refresh_start_dt = max(start_dt, end_dt - OPEN_RANGE_REFRESH_OVERLAP_BARS * bar_delta)
        tail_df = fetch_range_df(coin, quote, market, interval, exchange, refresh_start_dt, end_dt, callfast)
        updated_df = merge_cache_df(updated_df, tail_df)

    updated_df = normalize_cache_df(updated_df)
    return updated_df, slice_range_df(updated_df, start_dt, end_dt)


def fetch_recent_df(coin, quote, market, interval, length, exchange, callfast) -> pd.DataFrame:
    klines = get_recent_klines(coin, quote, market, interval, length, exchange, only_complete=True, callfast=callfast)
    return api_klines_to_cache_df(klines)


def fetch_range_df(coin, quote, market, interval, exchange, start_dt, end_dt, callfast) -> pd.DataFrame:
    start_arg = dt_to_request_str(start_dt)
    end_arg = dt_to_request_str(end_dt) if end_dt is not None else None
    klines = get_klines_range(coin, quote, market, interval, start_arg, end_arg, exchange, callfast=callfast)
    return api_klines_to_cache_df(klines)


def fetch_older_df(coin, quote, market, interval, exchange, end_before_dt, length, callfast) -> pd.DataFrame:
    if length <= 0:
        return empty_cache_df()

    unit_limit = fetch_unit_limit(exchange, market, callfast)
    remaining = int(length)
    next_end_dt = pd.Timestamp(end_before_dt)
    frames = []

    while remaining > 0:
        batch_size = min(remaining, unit_limit)
        if exchange == 'binance':
            klines = binance_klines_api.get_klines(
                coin,
                quote,
                market,
                interval,
                endTime=cache_dt_to_binance_end_ms(next_end_dt),
                length=batch_size,
            )
        elif exchange == 'upbit':
            klines = upbit_klines_api.get_klines(
                coin,
                quote,
                market,
                interval,
                endTime=cache_dt_to_upbit_end_str(next_end_dt),
                length=batch_size,
            )
        else:
            raise ValueError(f'Unsupported exchange for parquet source: {exchange}')

        batch_df = api_klines_to_cache_df(klines)
        if len(batch_df) == 0:
            break

        frames.append(batch_df)
        remaining -= len(batch_df)
        next_end_dt = batch_df.index.min()

        if len(batch_df) < batch_size:
            break

    if not frames:
        return empty_cache_df()

    return normalize_cache_df(pd.concat(frames))


def fetch_unit_limit(exchange, market, callfast):
    exchange = str(exchange).lower()
    market = str(market).lower()
    if exchange == 'binance':
        if market == 'spot':
            return 1000
        if market == 'swap':
            return 1500 if callfast else 499
    if exchange == 'upbit':
        return 200
    raise ValueError(f'Unsupported exchange/market for parquet source: {exchange}/{market}')


def slice_range_df(df: pd.DataFrame, start_dt, end_dt) -> pd.DataFrame:
    if len(df) == 0:
        return empty_cache_df()
    out = normalize_cache_df(df).loc[pd.Timestamp(start_dt):]
    if end_dt is not None:
        out = out.loc[:pd.Timestamp(end_dt)]
    return normalize_cache_df(out)


def split_missing_segments(missing_index: pd.DatetimeIndex, bar_delta: pd.Timedelta):
    segments = []
    if len(missing_index) == 0:
        return segments

    seg_start_dt = missing_index[0]
    prev_dt = missing_index[0]
    for curr_dt in missing_index[1:]:
        if pd.Timestamp(curr_dt) - pd.Timestamp(prev_dt) != bar_delta:
            segments.append((pd.Timestamp(seg_start_dt), pd.Timestamp(prev_dt)))
            seg_start_dt = curr_dt
        prev_dt = curr_dt
    segments.append((pd.Timestamp(seg_start_dt), pd.Timestamp(prev_dt)))
    return segments


def align_range_start(start, interval):
    return ceil_to_interval(to_timestamp(start), interval)


def align_range_end(end, interval):
    if end is None:
        return None
    return floor_to_interval(to_timestamp(end), interval)


def latest_complete_open_dt(interval):
    return floor_to_interval(pd.Timestamp.now(), interval) - _bar_delta(interval)


def trim_in_progress_cache_df(df: pd.DataFrame, interval) -> pd.DataFrame:
    if len(df) == 0:
        return empty_cache_df()
    latest_complete_dt = latest_complete_open_dt(interval)
    return normalize_cache_df(df).loc[:latest_complete_dt]


def floor_to_interval(dt, interval):
    bar_delta = _bar_delta(interval)
    ts = pd.Timestamp(dt)
    offset = (ts.value - pd.Timestamp('1970-01-01').value) // bar_delta.value
    return pd.Timestamp('1970-01-01') + offset * bar_delta


def ceil_to_interval(dt, interval):
    floored_dt = floor_to_interval(dt, interval)
    if floored_dt == pd.Timestamp(dt):
        return floored_dt
    return floored_dt + _bar_delta(interval)


def count_bars_between(start_dt, end_dt, bar_delta):
    if pd.Timestamp(end_dt) <= pd.Timestamp(start_dt):
        return 0
    return int((pd.Timestamp(end_dt) - pd.Timestamp(start_dt)) / bar_delta)


def dt_to_request_str(dt) -> str:
    return pd.Timestamp(dt).strftime('%Y-%m-%d %H:%M:%S')


def cache_dt_to_binance_end_ms(cache_dt) -> int:
    utc_like_dt = pd.Timestamp(cache_dt) - pd.Timedelta(hours=TZ_shift)
    return int((utc_like_dt - pd.Timestamp('1970-01-01')) / pd.Timedelta(milliseconds=1)) - 1


def cache_dt_to_upbit_end_str(cache_dt) -> str:
    utc_like_dt = pd.Timestamp(cache_dt) - pd.Timedelta(hours=TZ_shift)
    return utc_like_dt.strftime('%Y-%m-%dT%H:%M:%S')


def to_timestamp(dt_like):
    if isinstance(dt_like, pd.Timestamp):
        return dt_like
    if hasattr(dt_like, 'year') and hasattr(dt_like, 'month'):
        return pd.Timestamp(dt_like)
    return pd.Timestamp(convert_str_to_dt(str(dt_like)))


def _bar_delta(interval):
    return pd.Timedelta(minutes=interval_to_minute(interval))
