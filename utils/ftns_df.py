import numpy as np


def ffill_df_hour(df, length_hour, start=9):
    if length_hour > 24:
        raise ValueError('Not Implemented for hour greater than 24!')
    copy_df = df.copy()
    copy_df[(copy_df.index.minute > 0) | (copy_df.index.hour % length_hour != start % length_hour)] = np.nan
    return copy_df.ffill()


def ffill_df_week(df, start=9):
    copy_df = df.copy()
    copy_df[(copy_df.index.minute > 0) | (copy_df.index.hour != start)] = np.nan
    copy_df[copy_df.index.weekday > 0] = np.nan
    return copy_df.ffill()


def cs_neut(df):
    return df.sub(df.mean(axis=1), axis=0)


def cs_rank(df, neut=False):
    # -1 ~ 1 if neut
    # 0 ~ 1 else
    ranked_df = df.rank(axis=1, ascending=True)
    df_count = ranked_df.count(axis=1) - 1
    df_div = (ranked_df - 1.0).div(df_count, axis=0)
    result = (0.5 * ranked_df).where(df_div.isna(), df_div)

    result.replace(np.inf, np.nan, inplace=True)

    return 2 * cs_neut(result) if neut else result


def ts_rank(df, length):
    ranked_df = df.rolling(length).rank()
    return ranked_df


def cs_power(df, power):
    return np.power(df.abs(), power) * np.sign(df)


def cs_triangle(df):
    ranked_df = cs_rank(df, neut=True) * 2
    ranked_df[ranked_df >= 0.5] = 1 - ranked_df
    ranked_df[ranked_df <= -0.5] = -1 - ranked_df
    return ranked_df


def cs_scale(df, scale, ignore_tiny=False, max_mode=False):
    # max_mode -> bksize = min(original_bksize, scale)
    original_scale = df.abs().sum(axis=1)
    factor = scale / original_scale
    factor.replace([np.inf, -np.inf], np.nan, inplace=True)
    if ignore_tiny:
        src_book_size_ma = original_scale.rolling(100, min_periods=2).mean()
        factor[original_scale < 1e-5 * src_book_size_ma] = 0.0
    if max_mode:
        factor = factor.where(factor < 1.0, 1.0)  # if factor[di] > 1 -> factor[di] = 1, factor[di] = min(1, factor[di])

    return df.mul(factor, axis=0)
