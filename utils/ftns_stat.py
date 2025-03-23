from os import environ

import matplotlib
import numpy as np

from config.constants import simBK

if 'SHELL' in environ.keys():
    matplotlib.use('Agg')
else:
    matplotlib.use('Qt5Agg')
from matplotlib import pyplot as plt


def get_mdd(simRet):
    # not correct way since actual long-term return is much different from sum of returns
    r = simRet.cumsum()
    dd = r.subtract(r.cummax())
    mdd = dd.min()
    return float(mdd)


def get_sr(simRet, scale=365):
    return float(np.nanmean(simRet) / np.nanstd(simRet) * (scale ** 0.5))


def get_tvr(pos, simBK=simBK):
    tvrs = abs(pos.diff(1)).sum(axis=1) / simBK
    return float(tvrs[1:].mean())


def get_netBK(pos, simBK=simBK):
    netBK = abs(pos).sum(axis=1) / simBK
    return float(netBK[1:].mean())


def rolling_sr(simRet, length, scale):
    return simRet.rolling(length).mean() / simRet.rolling(length).std() * (scale ** 0.5)


def stat_to_text(stat):
    sr = stat.get('sr', 0)
    sr3 = stat.get('sr3', 0)
    sr7 = stat.get('sr7', 0)
    sr30 = stat.get('sr30', 0)
    sr90 = stat.get('sr90', 0)
    absr180 = stat.get('absr180', 0)
    ret = stat.get('ret', 0)
    tvr = stat.get('tvr', 0)
    mdd = stat.get('mdd', 0)
    sr_stat = f'SR:{sr:.2f}({sr90:.2f}, {sr30:.2f}, {sr7:.2f})'
    # etc_stat = f'ret:{100 * ret:.2f}%, tvr:{100 * tvr:.2f}% (r/t = {ret / tvr:.2f}%), mdd:{100 * mdd:.2f}%'
    etc_stat = f'r/t = {ret / tvr:.2f}% ({100 * ret:.2f}% / {100 * tvr:.2f}%), mdd:{100 * mdd:.2f}%'
    return f'{sr_stat}, {etc_stat}'


def get_stat(pos, simRet, scale=365, ee=False):
    def _abs_sr(simRet, length, scale=scale):
        return float(abs(rolling_sr(simRet, length, scale)).max())
    _day = int(scale / 365)
    if ee:
        trimmed_pos = pos
        trimmed_simRet = simRet
    else:
        trimmed_pos = pos[abs(pos).sum(axis=1) > 0]
        trimmed_simRet = simRet[abs(pos).sum(axis=1) > 0]
    absr3 = _abs_sr(simRet, _day * 3)
    absr7 = _abs_sr(simRet, _day * 7)
    absr14 = _abs_sr(simRet, _day * 14)
    absr30 = _abs_sr(simRet, _day * 30)
    absr90 = _abs_sr(simRet, _day * 90)
    absr180 = _abs_sr(simRet, _day * 180)
    sr = get_sr(trimmed_simRet, scale)
    sr3 = get_sr(trimmed_simRet[-3 * _day:], scale)
    sr7 = get_sr(trimmed_simRet[-7 * _day:], scale)
    sr30 = get_sr(trimmed_simRet[-30 * _day:], scale)
    sr90 = get_sr(trimmed_simRet[-90 * _day:], scale)
    mdd = get_mdd(trimmed_simRet)
    tvr_turn = get_tvr(trimmed_pos)
    tvr = tvr_turn * scale / 365  # daily tvr
    ret_turn = float(trimmed_simRet.mean())
    ret = ret_turn * scale
    return {'sr': sr, 'sr3': sr3, 'sr7': sr7, 'sr30': sr30, 'sr90': sr90,
            'absr3': absr3, 'absr7': absr7, 'absr14': absr14, 'absr30': absr30, 'absr90': absr90, 'absr180': absr180,
            'ret': ret, 'tvr': tvr, 'ret_turn': ret_turn, 'tvr_turn': tvr_turn, 'mdd': mdd}


def plot_series(series, title=None, fig_path=None):
    # fig = series.plot()
    # if title is not None:
    #     fig.suptitle(title, fontsize=10)
    fig = series.plot(title=title)
    if fig_path is None:
        # plt.show()
        plt.pause(5)
    elif fig_path is not False:
        fig.get_figure().savefig(fig_path)
    return fig


def simul(mdf, pos=None, buying_price='open', verbose=False, plot=True, fig_path=None, simBK=simBK, ee=False):
    _buying_field = mdf.get(buying_price)
    _buying_ret = _buying_field / _buying_field.shift(1) - 1.0
    _open_ret = mdf.open / mdf.open.shift(1) - 1.0
    simPnL = (pos.shift(1) * _open_ret.shift(-2) + pos.diff(1) * _buying_ret.shift(-2)).sum(axis=1)
    simRet = simPnL / simBK
    stat = get_stat(pos, simRet, scale=mdf.year, ee=ee)
    if plot:
        plot_series(simPnL.cumsum(), stat_to_text(stat), fig_path)
    if verbose:
        print('### stat')
        print(stat_to_text(stat))
    return stat, simRet
