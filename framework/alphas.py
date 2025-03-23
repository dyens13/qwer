from framework.operations import Operations
from utils.ftns_general import expr_replace, expr_replace_simple


class Alphas:
    @staticmethod
    def ret(mdf, n):
        pos = eval(f'mdf.returns{n}')

        pos = Operations.filterCount(pos)
        pos = Operations.rank(pos)
        pos = Operations.scale(pos)
        return pos

    @staticmethod
    def contango(mdf, n):
        pos = eval(f'mdf.MA{n}_close / mdf.MA{n}_xclose')

        pos = Operations.filterCount(pos)
        pos = Operations.rank(pos)
        pos = Operations.scale(pos)
        return pos

    @staticmethod
    def revMA(mdf, n):
        pos = eval(f'mdf.MA{n}_close - mdf.close')

        pos = Operations.filterCount(pos)
        pos = Operations.rank(pos)
        pos = Operations.scale(pos)
        return pos

    @staticmethod
    def expr_univ(mdf, expr, univ=30, univ_length=30, factor=None, **params):
        if factor:
            expr = f'({expr}) * ({factor})'
        pos = eval(expr_replace_simple(expr, 'mdf'))
        _univ_factor = f"#usd{int(mdf.day * univ_length)}"
        pos = Operations.filterRank(pos, mdf, _univ_factor, univ)
        for op in params.keys():
            # params : decay=24, normalize=1, etc...
            # print(f'{op} with {params[op]}')
            pos = eval(f'Operations.{op}(pos, {params[op]})')
        pos = Operations.filterCount(pos, count=5)
        pos = Operations.neut(pos)
        pos = Operations.scale(pos, max_mode=False)
        pos = Operations.nanToZero(pos)
        return pos


    @staticmethod
    def expr_usd(mdf, expr, usd_filter=1e7, factor=None, **params):
        if factor:
            expr = f'({expr}) * ({factor})'
        pos = eval(expr_replace_simple(expr, 'mdf'))
        pos = Operations.filterExpr(pos, mdf, f'#usd{mdf.week} > {7*usd_filter}', interval=mdf.week)
        for op in params.keys():
            # params : decay=24, normalize=1, etc...
            # print(f'{op} with {params[op]}')
            pos = eval(f'Operations.{op}(pos, {params[op]})')
        pos = Operations.filterCount(pos, count=5)
        pos = Operations.neut(pos)
        pos = Operations.scale(pos, max_mode=False)
        pos = Operations.nanToZero(pos)
        # pos = Operations.rollingWeight(pos, mdf, 24 * 30, 24 * 7, srcut=0)
        return pos

    @staticmethod
    def expr_pick(mdf, expr, rank=10, side=1, exclude=0):
        signal = eval(expr_replace_simple(expr, 'mdf'))
        pos = mdf.zeros.copy()
        univ_size = len(mdf.coins)
        # side : 1 -> long only, -1 -> short only, 0 -> both
        if side > 0:
            sig_rank = signal.rank(axis=1, ascending=False)
            pos[sig_rank <= rank] = 1
            pos[sig_rank <= exclude] = 0
        elif side < 0:
            sig_rank = signal.rank(axis=1, ascending=True)
            pos[sig_rank <= rank] = -1
            pos[sig_rank <= exclude] = 0
        else:
            sig_rank = signal.rank(axis=1, ascending=False)
            pos[sig_rank <= rank] = 1
            pos[sig_rank <= exclude] = 0
            sig_rank = signal.rank(axis=1, ascending=True)
            pos[sig_rank <= rank] = -1
            pos[sig_rank <= exclude] = 0
        pos = Operations.scale(pos, max_mode=False)
        pos = Operations.nanToZero(pos)
        return pos