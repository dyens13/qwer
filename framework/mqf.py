from config.constants import simBK
from framework.alphas import Alphas
from framework.mdf import MDF
from utils.ftns_general import load_yaml
from utils.ftns_stat import get_stat, plot_series, stat_to_text, simul


class Simulation:
    def __init__(self, coins, insts, interval, history, source, alphapool):
        self.df = MDF(coins, insts, stride=interval, history=history)
        self.alphapool = alphapool
        self._pos = None
        self._combos = None
        self._portfolio = None
        # if len(self.alphapool.keys()) > 0:
        #     self.set_stg()

    @classmethod
    def from_dict(cls, d):
        cts = d['constants']
        pts = d.get('alphapool', {})
        return cls(cts['univ'], cts['insts'], cts['stride'], cts['history'], cts['source'], pts)

    @classmethod
    def from_yaml(cls, yaml_path):
        d = load_yaml(yaml_path)
        return cls.from_dict(d)

    def set_pos(self, pos):
        self._pos = pos
        return None

    def set_pos_from_stg(self, stg):
        self._pos = stg(self.alphapool, self.df)
        return None

    @property
    def pos(self):
        if self._pos is not None:
            return self._pos
        else:
            raise NotImplementedError('set pos first!!')

    @property
    def pos_amt(self):
        return self.pos / self.df.close

    def simul(self, pos=None, buying_price='open', verbose=False, plot=True, fig_path=None, simBK=simBK, ee=False):
        if pos is None:
            pos = self.pos
        stat, simRet = simul(self.df, pos, buying_price, verbose, plot, fig_path, simBK, ee)
        return stat, simRet

    @property
    def netBK(self):
        return abs(self.pos).sum(axis=1)
