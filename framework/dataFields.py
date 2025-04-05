import numpy as np
import pandas as pd

MAX_PRICE_RATIO = 10.0
MIN_PRICE_RATIO = 0.2


class TSTransforms:
    @staticmethod
    def abs(o, n):
        return abs(o)

    @staticmethod
    def MA(o, n):
        return o.rolling(n, int(n/3)).mean()

    @staticmethod
    def tsStd(o, n):
        return o.rolling(n, int(n/3)).std()

    @staticmethod
    def tsZscore(o, n):
        return (o - o.rolling(n, int(n/3)).mean()) / o.rolling(n, int(n/3)).std()

    @staticmethod
    def high(o, n):
        return o.rolling(n, int(n/3)).max()

    @staticmethod
    def low(o, n):
        return o.rolling(n, int(n/3)).min()

    @staticmethod
    def stc(o, n):
        hi = o.rolling(n, int(n/3)).max()
        lo = o.rolling(n, int(n/3)).min()
        return (o-lo)/(hi-lo)


class DerivedFields:
    @staticmethod
    def vwap(o):
        return o.usd / o.amt

    @staticmethod
    def stc(o):
        return (o.close - o.low) / (o.high - o.low)

    @staticmethod
    def returns(o):
        return (o.close / o.close.shift(1)).clip(MIN_PRICE_RATIO, MAX_PRICE_RATIO) - 1.0

    @staticmethod
    def hml(o):
        return o.high - o.low

    @staticmethod
    def takerRatio(o):
        return o.takerUsd / o.usd

    @staticmethod
    def basis(o):
        return o.close / o.xclose - 1

class WindowedFields:
    @staticmethod
    def open(o, n):
        if n == 1:
            return o.open
        return o.open.fillna(method='bfill', limit=n - 1).shift(n - 1)

    @staticmethod
    def high(o, n):
        if n == 1:
            return o.high
        return o.high.rolling(n, min_periods=1).max()

    @staticmethod
    def low(o, n):
        if n == 1:
            return o.low
        return o.low.rolling(n, min_periods=1).min()

    @staticmethod
    def close(o, n):
        if n == 1:
            return o.close
        return o.close.fillna(method='ffill', limit=n - 1)

    @staticmethod
    def amt(o, n):
        if n == 1:
            return o.amt
        return o.amt.rolling(n, min_periods=1).sum()

    @staticmethod
    def usd(o, n):
        if n == 1:
            return o.usd
        return o.usd.rolling(n, min_periods=1).sum()

    @staticmethod
    def takerAmt(o, n):
        if n == 1:
            return o.takerAmt
        return o.takerAmt.rolling(n, min_periods=1).sum()

    @staticmethod
    def takerUsd(o, n):
        if n == 1:
            return o.takerUsd
        return o.takerUsd.rolling(n, min_periods=1).sum()

    @staticmethod
    def vwap(o, n):
        if n == 1:
            return o.vwap
        return eval(f'o.usd{n} / o.amt{n}')

    @staticmethod
    def twap(o, n):
        if n == 1:
            return o.twap
        return 0.25 * eval(f'(o.high{n} + o.low{n} + o.close{n} + o.open{n})')

    @staticmethod
    def stc(o, n):
        if n == 1:
            return o.stc
        return eval(f'(o.close - o.low{n}) / (o.high{n} - o.low{n})')

    @staticmethod
    def returns(o, n):
        if n == 1:
            return o.returns
        return (o.close / o.close.shift(n)).clip(MIN_PRICE_RATIO, MAX_PRICE_RATIO) - 1.0

    @staticmethod
    def hml(o, n):
        if n == 1:
            return o.hml
        return eval(f'o.high{n} - o.low{n}')

    @staticmethod
    def takerRatio(o, n):
        return o.takerUsd.rolling(n).sum() / o.usd.rolling(n).sum()

    @staticmethod
    def basis(o, n):
        return o.basis.rolling(n).mean()