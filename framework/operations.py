from config.constants import simBK
from utils.ftns_df import cs_rank, cs_neut, cs_scale, cs_power, cs_triangle, ffill_df_hour, ffill_df_week, ts_rank
from utils.ftns_general import expr_replace, expr_replace_simple


class Operations:

    @staticmethod
    def scale(pos, scale=simBK, max_mode=False):
        return cs_scale(pos, scale, ignore_tiny=True, max_mode=max_mode)

    @staticmethod
    def nanToZero(pos):
        return pos.fillna(0)

    @staticmethod
    def neut(pos):
        return cs_neut(pos)

    @staticmethod
    def rank(pos, neut=True):
        return cs_rank(pos, neut)

    @staticmethod
    def ts_rank(pos, length):
        return ts_rank(pos, length)

    @staticmethod
    def rank_pow(pos, power):
        return cs_power(cs_rank(pos, True), power)

    @staticmethod
    def rank_qtl(pos, quantile, side=0):
        # side : 1 -> long only, -1 -> short only, 0 -> both
        if side == 0:
            pos = cs_rank(pos, True)
            pos[abs(pos) < 1 - 2*quantile] = 0
            # pos = Operations.scale(pos, max_mode=False)
            # pos = Operations.nanToZero(pos)
        elif side > 0:
            pos = cs_rank(pos)
            pos[pos < 1 - quantile] = 0
            # pos = Operations.scale(pos, max_mode=False)
            # pos = Operations.nanToZero(pos)
        elif side < 0:
            pos = cs_rank(pos)
            pos[pos > quantile] = 0
        return pos

    @staticmethod
    def normalize(pos, method=1):
        if method == 1:
            return cs_rank(pos, True)
        elif method > 0:
            return cs_power(cs_rank(pos, True), method)
        elif method < 0:
            return cs_triangle(pos)

    @staticmethod
    def hold(pos, mdf, interval=1):
        if interval == mdf.week:
            pos = ffill_df_week(pos)
        elif interval > mdf.day:
            raise NotImplementedError('only for week or several hours')
        elif interval > 1:
            pos = ffill_df_hour(pos, interval)
        return pos

    @staticmethod
    def filterCount(pos, count=2):
        return pos.where(pos.count(axis=1) >= count)

    @staticmethod
    def filterExpr(pos, mdf, expr, interval=1):
        # _filter = eval(expr_replace(expr, 'mdf'))
        _filter = eval(expr_replace_simple(expr, 'mdf'))
        if interval == mdf.week:
            _filter = ffill_df_week(_filter)
        elif interval > mdf.day:
            raise NotImplementedError('only for week or several hours')
        elif interval > 1:
            _filter = ffill_df_hour(_filter, interval)
        return pos.where(_filter)

    @staticmethod
    def filterQtl(pos, mdf, field, cond, interval=1):
        _vol = eval(f'mdf.{field}')
        ranked_field = cs_rank(_vol)  # filterQtl(usd, quantile=0.1)
        _filter = eval(f'ranked_field {cond}')
        if interval == mdf.week:
            _filter = ffill_df_week(_filter)
        elif interval > mdf.day:
            raise NotImplementedError('only for week or several hours')
        elif interval > 1:
            _filter = ffill_df_hour(_filter, interval)
        return pos.where(_filter)

    @staticmethod
    def filterRank(pos, mdf, expr, univ=10):
        _vol = eval(expr_replace_simple(expr, 'mdf'))
        _vol_rank = _vol.rank(axis=1, ascending=False)
        _vol_rank = ffill_df_week(_vol_rank)
        return pos.where(_vol_rank <= univ)

    @staticmethod
    def qtlNeut(pos, mdf, expr, qtl=2, interval=1):
        if qtl == 1:
            return cs_neut(pos)
        res_pos = pos.copy()
        ranked_field = cs_rank(eval(expr_replace_simple(expr, 'mdf')))
        # ranked_field = cs_rank(eval(expr_replace(expr, 'mdf')))
        if interval == mdf.week:
            ranked_field = ffill_df_week(ranked_field)
        elif interval > mdf.day:
            raise NotImplementedError('only for week or several hours')
        elif interval > 1:
            ranked_field = ffill_df_hour(ranked_field, interval)
        qtl_delta = 1.0 / qtl
        qtl_pos = res_pos[ranked_field >= 1.0 - qtl_delta]
        res_pos[ranked_field >= 1.0 - qtl_delta] = qtl_pos.sub(qtl_pos.mean(axis=1), axis=0)
        for n in range(qtl-1):
            qtl_pos = res_pos[(ranked_field < 1.0 - (n+1)*qtl_delta) & (ranked_field >= 1.0 - (n+2)*qtl_delta)]
            res_pos[(ranked_field < 1.0 - (n+1)*qtl_delta) & (ranked_field >= 1.0 - (n+2)*qtl_delta)] = qtl_pos.sub(qtl_pos.mean(axis=1), axis=0)
        return res_pos

    @staticmethod
    def eventNeut(pos, mdf, expr):
        res_pos = pos.copy()
        event = eval(expr_replace(expr, 'mdf'))
        for tf in set(event.values.ravel()):
            res_pos[event == tf] = res_pos[event == tf].sub(res_pos[event == tf].mean(axis=1), axis=0)
        return res_pos

    @staticmethod
    def decay_unif(pos, days=2):
        if days == 1:
            return pos
        else:
            return pos.rolling(days).mean()

    @staticmethod
    def decay_exp(pos, days=2):
        if days == 1:
            return pos
        else:
            new_pos = days * pos
            for i in range(1, days):
                new_pos += (days-i) * pos.shift(i)
            return new_pos / (days*(days+1)/2)

    @staticmethod
    def qtl_pick(pos, quantile, side=0, usd_filter=1e7, **params):
        # side : 1 -> long only, -1 -> short only, 0 -> both
        # fixme
        if side == 0:
            pos = Operations.rank(pos)
            pos[abs(pos) < 1 - 2*quantile] = 0
            pos[pos > 0] = 1
            pos[pos < 0] = -1
            pos = Operations.scale(pos, max_mode=False)
            pos = Operations.nanToZero(pos)
        elif side == 1:
            pos = Operations.rank(pos, False)
            pos[pos < 1 - quantile] = 0
            pos[pos > 1 - quantile] = 1
            pos = Operations.scale(pos, max_mode=False)
            pos = Operations.nanToZero(pos)
        return pos

    @staticmethod
    def rank_pick(pos, rank=10, side=1):
        # side : 1 -> long only, -1 -> short only, 0 -> both
        if side > 0:
            sig_rank = pos.rank(axis=1, ascending=False)
            pos[sig_rank > rank] = 0
            pos[sig_rank <= rank] = 1
            pos = Operations.scale(pos, max_mode=False)
            pos = Operations.nanToZero(pos)
        elif side < 0:
            raise NotImplementedError('use long only')
        else:
            sig_rank = pos.rank(axis=1, ascending=False)
            pos[sig_rank <= rank] = 1
            pos[sig_rank > rank] = 0
            sig_rank = pos.rank(axis=1, ascending=True)
            pos[sig_rank <= rank] = 1
            pos = Operations.scale(pos, max_mode=False)
            pos = Operations.nanToZero(pos)
        return pos
