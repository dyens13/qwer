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
        return o.rolling(n, int(n / 3)).mean()

    @staticmethod
    def uB(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() + rolling.std()

    @staticmethod
    def lB(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() - rolling.std()

    @staticmethod
    def u2B(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() + 2 * rolling.std()

    @staticmethod
    def l2B(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() - 2 * rolling.std()

    @staticmethod
    def u3B(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() + 3 * rolling.std()

    @staticmethod
    def l3B(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() - 3 * rolling.std()

    @staticmethod
    def u4B(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() + 4 * rolling.std()

    @staticmethod
    def l4B(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() - 4 * rolling.std()

    @staticmethod
    def tsStd(o, n):
        return o.rolling(n, int(n / 3)).std()

    @staticmethod
    def tsZscore(o, n):
        rolling = o.rolling(n, int(n / 3))
        return (o - rolling.mean()) / rolling.std()

    @staticmethod
    def tsMts(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.mean() / rolling.std()

    @staticmethod
    def tsXplus(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.sum() / abs(o).rolling(n, int(n / 3)).sum()

    @staticmethod
    def tsXsgn(o, n):
        sgn = np.sign(o)
        return sgn.rolling(n, int(n / 3)).sum() / abs(sgn).rolling(n, int(n / 3)).sum()

    @staticmethod
    def tsCorrline(o, n):
        idx = pd.Series(np.arange(len(o)), index=o.index)
        return o.rolling(n, int(n / 3)).corr(idx, pairwise=True)

    @staticmethod
    def high(o, n):
        return o.rolling(n, int(n / 3)).max()

    @staticmethod
    def low(o, n):
        return o.rolling(n, int(n / 3)).min()

    @staticmethod
    def hml(o, n):
        rolling = o.rolling(n, int(n / 3))
        return rolling.max() - rolling.min()

    @staticmethod
    def hmc(o, n):
        return o.rolling(n, int(n / 3)).max() - o

    @staticmethod
    def cml(o, n):
        return o - o.rolling(n, int(n / 3)).min()

    @staticmethod
    def diff(o, n):
        return o.diff(n)

    @staticmethod
    def slope(o, n):
        return o.diff(n) / n

    @staticmethod
    def stc(o, n):
        hi = o.rolling(n, int(n / 3)).max()
        lo = o.rolling(n, int(n / 3)).min()
        return (o - lo) / (hi - lo)


class DerivedFields:
    @staticmethod
    def vwap(o):
        return o.usd / o.amt

    @staticmethod
    def lvwap(o):
        return o.takerUsd / o.takerAmt

    @staticmethod
    def svwap(o):
        return (o.usd - o.takerUsd) / (o.amt - o.takerAmt)

    @staticmethod
    def dvwap(o):
        return o.lvwap - o.svwap

    @staticmethod
    def twap(o):
        return 0.25 * (o.high + o.low + o.close + o.open)

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
    def cml(o):
        return o.close - o.low

    @staticmethod
    def hmc(o):
        return o.high - o.close

    @staticmethod
    def cmo(o):
        return o.close - o.open

    @staticmethod
    def cmv(o):
        return o.close - o.vwap

    @staticmethod
    def amp(o):
        return abs(o.cmo) / o.hml

    @staticmethod
    def vcr(o):
        return o.m('swap').usd / o.m('spot').usd

    @staticmethod
    def lvcr(o):
        return np.log(o.m('swap').usd / o.m('spot').usd)

    @staticmethod
    def takerRatio(o):
        return o.takerUsd / o.usd

    @staticmethod
    def basis(o):
        return o.m('swap').close / o.m('spot').close - 1

    @staticmethod
    def sqz(o):
        return o.m('swap').hml / o.m('spot').hml


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
    def lvwap(o, n):
        if n == 1:
            return o.lvwap
        return eval(f'o.takerUsd{n} / o.takerAmt{n}')

    @staticmethod
    def svwap(o, n):
        if n == 1:
            return o.svwap
        return eval(f'(o.usd{n} - o.takerUsd{n}) / (o.amt{n} - o.takerAmt{n})')

    @staticmethod
    def dvwap(o, n):
        if n == 1:
            return o.dvwap
        return eval(f'o.lvwap{n} - o.svwap{n}')

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
    def cml(o, n):
        if n == 1:
            return o.cml
        return eval(f'o.close - o.low{n}')

    @staticmethod
    def hmc(o, n):
        if n == 1:
            return o.hmc
        return eval(f'o.high{n} - o.close')

    @staticmethod
    def cmo(o, n):
        if n == 1:
            return o.cmo
        return eval(f'o.close - o.open{n}')

    @staticmethod
    def cmv(o, n):
        if n == 1:
            return o.cmv
        return eval(f'o.close - o.vwap{n}')

    @staticmethod
    def amp(o, n):
        if n == 1:
            return o.amp
        return eval(f'abs(o.cmo{n}) / o.hml{n}')

    @staticmethod
    def mom(o, n):
        return o.cml.rolling(n).sum() / o.hml.rolling(n).sum()

    @staticmethod
    def vcr(o, n):
        return eval(f"o.m('swap').usd{n} / o.m('spot').usd{n}")

    @staticmethod
    def lvcr(o, n):
        return np.log(eval(f"o.m('swap').usd{n} / o.m('spot').usd{n}"))

    @staticmethod
    def takerRatio(o, n):
        return o.takerUsd.rolling(n).sum() / o.usd.rolling(n).sum()

    @staticmethod
    def basis(o, n):
        return o.basis.rolling(n).mean()

    @staticmethod
    def sqz(o, n):
        return eval(f"o.m('swap').hml{n} / o.m('spot').hml{n}")

    @staticmethod
    def zsc(o, n):
        rolling = o.close.rolling(n, int(n / 3))
        return (o.close - rolling.mean()) / rolling.std()

    @staticmethod
    def lzsc(o, n):
        rolling = o.close.rolling(n, int(n / 3))
        return (o.low - rolling.mean()) / rolling.std()

    @staticmethod
    def hzsc(o, n):
        rolling = o.close.rolling(n, int(n / 3))
        return (o.high - rolling.mean()) / rolling.std()
