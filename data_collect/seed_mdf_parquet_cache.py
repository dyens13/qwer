from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def setup_runtime() -> None:
    for stream_name in ('stdout', 'stderr'):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, 'reconfigure', None)
        if callable(reconfigure):
            reconfigure(encoding='utf-8', errors='replace')


from config.constants import DATA_CACHE_DIR
from data_collect.binance.klines_api import get_available_coins
from data_collect.klines_parquet_cache import get_cache_path, latest_complete_open_dt, read_cache_df, write_cache_df
from framework.mdf import get_df, resolve_coin_name_and_scale


DEFAULT_EXCHANGE = 'binance'
DEFAULT_MARKETS = ('spot', 'swap')
DEFAULT_QUOTE = 'USDT'
DEFAULT_START = '2017-01-01 00:00:00'
DEFAULT_STRIDES = ('15m', '1h')
ANCHOR_COIN = 'BTC'
ANCHOR_MARKET = 'spot'
ANCHOR_STRIDE = '1h'
LAST_COIN = 'ETH'
STRIDE_PRIORITY = {'1h': 0, '15m': 1}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Seed MDF parquet cache with the widest practical history for 15m/1h candles.'
    )
    parser.add_argument('--exchange', default=DEFAULT_EXCHANGE, choices=['binance'])
    parser.add_argument('--market', default=None, choices=['spot', 'swap'])
    parser.add_argument('--markets', nargs='+', default=None, choices=['spot', 'swap'])
    parser.add_argument('--quote', default=DEFAULT_QUOTE)
    parser.add_argument('--start', default=DEFAULT_START)
    parser.add_argument('--strides', nargs='+', default=list(DEFAULT_STRIDES), choices=['15m', '1h'])
    parser.add_argument('--coins', nargs='*', default=None)
    parser.add_argument('--limit-coins', type=int, default=None)
    parser.add_argument('--slow', action='store_true')
    return parser.parse_args()


def normalize_markets(args: argparse.Namespace) -> list[str]:
    if args.market is not None:
        return [str(args.market).lower()]
    if args.markets:
        return list(dict.fromkeys(str(market).lower() for market in args.markets))
    return list(DEFAULT_MARKETS)


def load_coin_universe(market: str, quote: str, user_coins: list[str] | None, limit_coins: int | None) -> list[str]:
    market_coins = [str(coin).upper() for coin in get_available_coins(market, quote=quote)]
    if user_coins:
        requested = [str(coin).upper() for coin in user_coins]
        coins = []
        for coin in requested:
            try:
                resolve_coin_name_and_scale(coin, market_coins)
            except ValueError:
                continue
            coins.append(coin)
    else:
        coins = market_coins

    unique_coins = sorted(dict.fromkeys(coins))
    if limit_coins is not None:
        unique_coins = unique_coins[:int(limit_coins)]
    return unique_coins


def prioritize_coins(coins: list[str]) -> list[str]:
    unique_coins = list(dict.fromkeys(str(coin).upper() for coin in coins))
    return sorted(
        unique_coins,
        key=lambda coin: (
            coin != ANCHOR_COIN,
            coin == LAST_COIN,
            coin,
        ),
    )


def prioritize_strides(strides: list[str]) -> list[str]:
    return sorted(dict.fromkeys(str(stride) for stride in strides), key=lambda stride: (STRIDE_PRIORITY.get(stride, 999), stride))


def trim_cache_file_to_end(cache_path: Path, end: str) -> pd.DataFrame:
    cache_df = read_cache_df(cache_path)
    if len(cache_df) == 0:
        return cache_df
    capped_df = cache_df.loc[:pd.Timestamp(end)]
    write_cache_df(cache_path, capped_df)
    return capped_df


def build_seed_tasks(market_coin_map: dict[str, list[str]], strides: list[str]) -> list[tuple[str, str, str]]:
    tasks: list[tuple[str, str, str]] = []
    for stride in prioritize_strides(strides):
        for market, coins in market_coin_map.items():
            for coin in prioritize_coins(coins):
                if market == ANCHOR_MARKET and stride == ANCHOR_STRIDE and coin == ANCHOR_COIN:
                    continue
                tasks.append((market, coin, stride))
    return tasks


def format_seconds(seconds: float) -> str:
    if seconds < 60:
        return f'{seconds:.1f}s'
    minutes = int(seconds // 60)
    seconds_left = int(seconds % 60)
    if minutes < 60:
        return f'{minutes}m {seconds_left}s'
    hours = minutes // 60
    minutes_left = minutes % 60
    return f'{hours}h {minutes_left}m'


def estimate_eta(start_ts: float, completed: int, total: int) -> str:
    if completed <= 0:
        return 'unknown'
    elapsed = time.monotonic() - start_ts
    avg = elapsed / completed
    remaining = max(total - completed, 0) * avg
    return format_seconds(remaining)


def seed_one_coin_stride(
    coin: str,
    market: str,
    quote: str,
    exchange: str,
    stride: str,
    start: str,
    available_coins: list[str],
    callfast: bool,
    end: str | None = None,
) -> dict[str, object]:
    start_ts = time.monotonic()
    resolved_coin, _ = resolve_coin_name_and_scale(coin, available_coins)
    effective_end = end or latest_complete_open_dt(stride).strftime('%Y-%m-%d %H:%M:%S')
    df = get_df(
        coin=coin,
        quote=quote,
        market=market,
        stride=stride,
        exchange=exchange,
        source='parquet',
        coin_list=available_coins,
        callfast=callfast,
        start=start,
        end=effective_end,
    )
    cache_path = get_cache_path(exchange, market, quote, stride, resolved_coin)
    trimmed_cache_df = trim_cache_file_to_end(cache_path, effective_end)
    elapsed = time.monotonic() - start_ts
    return {
        'coin': coin,
        'resolved_coin': resolved_coin,
        'market': market,
        'stride': stride,
        'rows': int(len(df)),
        'first_dt': df.index.min(),
        'last_dt': trimmed_cache_df.index.max() if len(trimmed_cache_df) > 0 else None,
        'end': effective_end,
        'cache_path': cache_path,
        'cache_size': cache_path.stat().st_size if cache_path.exists() else None,
        'elapsed': elapsed,
    }


def main() -> int:
    setup_runtime()
    args = parse_args()

    markets = normalize_markets(args)
    market_coin_map = {
        market: load_coin_universe(market, args.quote, args.coins, args.limit_coins)
        for market in markets
    }
    non_empty_market_coin_map = {market: coins for market, coins in market_coin_map.items() if coins}
    if not non_empty_market_coin_map:
        print('[ERROR] no coins found for the requested universe', flush=True)
        return 1

    callfast = not args.slow
    tasks = build_seed_tasks(non_empty_market_coin_map, args.strides)
    total_tasks = len(tasks) + 1
    anchor_coin_list = [str(coin).upper() for coin in get_available_coins(ANCHOR_MARKET, quote=args.quote)]

    print('=' * 100, flush=True)
    print('[INFO] Seed MDF parquet cache', flush=True)
    print(f'[INFO] exchange={args.exchange} markets={markets} quote={args.quote}', flush=True)
    print(
        f"[INFO] start={args.start} strides={args.strides} "
        f"market_coin_counts={{{', '.join(f'{market}: {len(coins)}' for market, coins in non_empty_market_coin_map.items())}}} "
        f'callfast={callfast}',
        flush=True,
    )
    print(f'[INFO] cache_root={DATA_CACHE_DIR}', flush=True)
    print(f'[INFO] anchor={ANCHOR_COIN} {ANCHOR_MARKET} {ANCHOR_STRIDE}', flush=True)
    print(f'[INFO] total_tasks={total_tasks}', flush=True)
    print('=' * 100, flush=True)

    started_at = time.monotonic()
    success_count = 0
    failed: list[dict[str, str]] = []

    print(
        f'[PROGRESS] task 1/{total_tasks} role=anchor market={ANCHOR_MARKET} coin={ANCHOR_COIN} stride={ANCHOR_STRIDE} eta=unknown',
        flush=True,
    )
    try:
        anchor_result = seed_one_coin_stride(
            coin=ANCHOR_COIN,
            market=ANCHOR_MARKET,
            quote=args.quote,
            exchange=args.exchange,
            stride=ANCHOR_STRIDE,
            start=args.start,
            available_coins=anchor_coin_list,
            callfast=callfast,
            end=None,
        )
    except Exception as exc:
        print(f'[FAIL] role=anchor market={ANCHOR_MARKET} coin={ANCHOR_COIN} stride={ANCHOR_STRIDE} error={exc}', flush=True)
        return 2

    success_count += 1
    anchor_end = str(anchor_result['last_dt'])
    print(
        '[DONE] '
        f"role=anchor market={anchor_result['market']} coin={anchor_result['coin']} resolved={anchor_result['resolved_coin']} "
        f"stride={anchor_result['stride']} rows={anchor_result['rows']} first={anchor_result['first_dt']} "
        f"last={anchor_result['last_dt']} end={anchor_result['end']} "
        f"cache_size={anchor_result['cache_size']} elapsed={format_seconds(float(anchor_result['elapsed']))}",
        flush=True,
    )
    print(f'[INFO] anchor_end={anchor_end}', flush=True)

    for task_index, (market, coin, stride) in enumerate(tasks, start=1):
        eta = estimate_eta(started_at, success_count + len(failed), total_tasks)
        print(
            f'[PROGRESS] task {task_index + 1}/{total_tasks} market={market} coin={coin} stride={stride} anchor_end={anchor_end} eta={eta}',
            flush=True,
        )
        try:
            result = seed_one_coin_stride(
                coin=coin,
                market=market,
                quote=args.quote,
                exchange=args.exchange,
                stride=stride,
                start=args.start,
                available_coins=non_empty_market_coin_map[market],
                callfast=callfast,
                end=anchor_end,
            )
        except Exception as exc:
            failed.append({'market': market, 'coin': coin, 'stride': stride, 'error': str(exc)})
            print(f'[FAIL] market={market} coin={coin} stride={stride} error={exc}', flush=True)
            continue

        success_count += 1
        print(
            '[DONE] '
            f"market={result['market']} coin={result['coin']} resolved={result['resolved_coin']} stride={result['stride']} rows={result['rows']} "
            f"first={result['first_dt']} last={result['last_dt']} end={result['end']} "
            f"cache_size={result['cache_size']} elapsed={format_seconds(float(result['elapsed']))}",
            flush=True,
        )

    total_elapsed = time.monotonic() - started_at
    print('=' * 100, flush=True)
    print(
        f'[SUMMARY] success={success_count} failed={len(failed)} total_tasks={total_tasks} anchor_end={anchor_end} elapsed={format_seconds(total_elapsed)}',
        flush=True,
    )
    if failed:
        print('[SUMMARY] failed tasks:', flush=True)
        for item in failed:
            print(
                f"  - market={item['market']} coin={item['coin']} stride={item['stride']} error={item['error']}",
                flush=True,
            )
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
