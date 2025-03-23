from datetime import datetime, timedelta

import numpy as np
import pandas as pd

DATETIME_FORMAT_SHORT = '%y%m%d%H%M'
DATETIME_FORMAT_LONG = '%Y-%m-%dT%H:%M:%S.%fZ'
DATETIME_FORMAT_LONG_NOMILLISEC = '%Y-%m-%dT%H:%M:%SZ'
DATETIME_FORMAT_SQL = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%y%m%d'


def dt_from_timestamp(timestamp):
    # datetime.fromtimestamp(1*10e8) : (2001, 9, 9, 10, 46, 40)
    # datetime.fromtimestamp(1*10e9) : (2286, 11, 21, 2, 46, 40)
    if timestamp > 10e9:
        return datetime.fromtimestamp(int(timestamp) / 1000)
    elif timestamp < 10e7:
        return datetime.fromtimestamp(int(timestamp) * 1000)
    else:
        return datetime.fromtimestamp(int(timestamp))


def dt_to_short_dtstr(dt):
    return dt.strftime(DATETIME_FORMAT_SHORT)


def logging_dt(dt=None):
    if dt is None:
        dt = datetime.now()
    return dt.strftime(DATETIME_FORMAT_SQL)


def short_dtstr_to_dt(short_dtstr, minute=None):
    if minute is not None:
        if minute >= 60:
            raise ValueError('minute must be less than 60')
        return datetime.strptime(short_dtstr[:-2] + f'{minute:02.0f}', DATETIME_FORMAT_SHORT)
    else:
        return datetime.strptime(short_dtstr, DATETIME_FORMAT_SHORT)


def dt_to_long_dtstr(dt, second=None):
    if second is not None:
        if second >= 60:
            raise ValueError('second must be less than 60')
        return dt.strftime(DATETIME_FORMAT_LONG)[:-10] + f'{second:02.0f}.000Z'
    else:
        return dt.strftime(DATETIME_FORMAT_LONG)[:-4] + 'Z'


def dt_to_sql_dtstr(dt):
    return dt.strftime(DATETIME_FORMAT_SQL)


def get_last_day(dt, offset):
    new_dt = datetime(dt.year, dt.month, dt.day) + timedelta(hours=offset)
    if dt < new_dt:
        return new_dt - timedelta(days=1)
    else:
        return new_dt


def get_last_hour(dt, offset):
    new_dt = datetime(dt.year, dt.month, dt.day, dt.hour) + timedelta(minutes=offset)
    if dt < new_dt:
        return new_dt - timedelta(hours=1)
    else:
        return new_dt


def get_last_minute(dt, offset):
    new_dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) + timedelta(seconds=offset)
    if dt < new_dt:
        return new_dt - timedelta(minutes=1)
    else:
        return new_dt


def get_next_day(dt, offset):
    new_dt = datetime(dt.year, dt.month, dt.day) + timedelta(hours=offset)
    if dt < new_dt:
        return new_dt
    else:
        return new_dt + timedelta(days=1)


def get_next_hour(dt, offset):
    new_dt = datetime(dt.year, dt.month, dt.day, dt.hour) + timedelta(minutes=offset)
    if dt < new_dt:
        return new_dt
    else:
        return new_dt + timedelta(hours=1)


def get_next_stride(dt, stride=15):
    new_minute = (dt.minute // stride + 1) * stride
    new_dt = datetime(dt.year, dt.month, dt.day, dt.hour) + timedelta(minutes=new_minute)
    return new_dt


def get_next_minute(dt, offset):
    new_dt = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) + timedelta(seconds=offset)
    if dt < new_dt:
        return new_dt
    else:
        return new_dt + timedelta(minutes=1)


def tiny_forward_shift(dtstr):
    dt = datetime.strptime(dtstr, DATETIME_FORMAT_LONG) + timedelta(seconds=1)
    return dt.strftime(DATETIME_FORMAT_LONG)[:-4] + 'Z'


def tiny_backward_shift(dtstr):
    dt = datetime.strptime(dtstr, DATETIME_FORMAT_LONG) - timedelta(seconds=1)
    return dt.strftime(DATETIME_FORMAT_LONG)[:-4] + 'Z'


def get_real_start_dt(start, interval, offset):
    start_dt = datetime.strptime(start, DATETIME_FORMAT_LONG)

    if interval == 60:
        return get_next_minute(start_dt, offset)
    elif interval == 3600:
        return get_next_hour(start_dt, offset)
    elif interval == 86400:
        return get_next_day(start_dt, offset)
    else:
        raise ValueError('interval must be 86400, 3600, or 60.')


def get_real_start_dt_from_dt(start_dt, interval, offset):
    if interval == 60:
        return get_next_minute(start_dt, offset)
    elif interval == 3600:
        return get_next_hour(start_dt, offset)
    elif interval == 86400:
        return get_next_day(start_dt, offset)
    else:
        raise ValueError('interval must be 86400, 3600, or 60.')


def get_real_end_dt(end, interval, offset):
    end_dt = datetime.strptime(end, DATETIME_FORMAT_LONG)

    if interval == 60:
        return get_last_minute(end_dt, offset)
    elif interval == 3600:
        return get_last_hour(end_dt, offset)
    elif interval == 86400:
        return get_last_day(end_dt, offset)
    else:
        raise ValueError('interval must be 86400, 3600, or 60.')


def utc_dt_to_timestamp(dt):
    return int((dt - datetime(1970, 1, 1)) / timedelta(seconds=1))


def convert_str_to_dt(dt_info):
    if len(dt_info) == 10:  # 2019-07-26
        if dt_info[4] == dt_info[7] == '-':
            dt = datetime.strptime(dt_info, "%Y-%m-%d")
        else:  # 1907260800
            dt = datetime.strptime(dt_info, DATETIME_FORMAT_SHORT)
    elif len(dt_info) == 8:  # 20190726
        dt = datetime.strptime(dt_info, '%Y%m%d')
    elif len(dt_info) == 6:  # 190726
        dt = datetime.strptime(dt_info, '%y%m%d')
    elif len(dt_info) == 16:  # 2019-07-26 00:00
        dt = datetime.strptime(dt_info, '%Y-%m-%d %H:%M')
    elif len(dt_info) == 19:  # 2019-07-26 00:00:00 or 2019-07-26T00:00:00
        if 'T' in dt_info:
            dt = datetime.strptime(dt_info+'Z', DATETIME_FORMAT_LONG_NOMILLISEC)
        else:
            dt = datetime.strptime(dt_info, DATETIME_FORMAT_SQL)
    elif len(dt_info) == 20:  # 2020-07-27T05:44:23Z
        dt = datetime.strptime(dt_info, DATETIME_FORMAT_LONG_NOMILLISEC)
    elif len(dt_info) == 24:  # 2019-07-26T08:00:00.000Z
        dt = datetime.strptime(dt_info, DATETIME_FORMAT_LONG)
    elif len(dt_info) == 27:  # 2019-07-26T08:00:00.000Z
        dt = datetime.strptime(dt_info, DATETIME_FORMAT_LONG)
    elif len(dt_info) == 30:  # 2021-01-22T11:31:06.942213444Z
        dt_info_no_nanosec = dt_info[:-4] + 'Z'
        dt = datetime.strptime(dt_info_no_nanosec, DATETIME_FORMAT_LONG)
    else:
        raise ValueError(f'Unable to convert {dt_info} to datetime')
    return dt


def timestamp_unit_change(timestamp, unit):
    unit_dict = {'seconds': 10, 'milliseconds': 13, 'microseconds': 16, 'nanoseconds': 19}
    # seconds : 10 digits
    # milliseconds : 13 digits
    # microseconds : 16 digits
    # nanoseconds : 19 digits
    before_digits = len(str(timestamp))
    if before_digits not in [10, 13, 16, 19]:
        raise TypeError(f'Not available timestamp {timestamp}')
    return timestamp * int(10 ** (unit_dict[unit] - before_digits))


def convert_utc_to_timestamp(dt_info):
    if isinstance(dt_info, int):
        return dt_info
    if isinstance(dt_info, np.int64):
        return int(dt_info)
    if isinstance(dt_info, datetime):
        return utc_dt_to_timestamp(dt_info)
    if isinstance(dt_info, str):
        dt = convert_str_to_dt(dt_info)
        return utc_dt_to_timestamp(dt)
    raise TypeError('Can convert only datetime objects or numeric string objects of length, 6, 8, 10, or 24.')


def convert_to_timestamp_old(dt_info):
    if isinstance(dt_info, int):
        return dt_info
    if isinstance(dt_info, np.int64):
        return int(dt_info)
    if isinstance(dt_info, (datetime, pd.Timestamp)):
        return int(dt_info.timestamp())
    if isinstance(dt_info, str):
        dt = convert_str_to_dt(dt_info)
        return int(dt.timestamp())
    raise TypeError('Can convert only datetime objects or numeric string objects of length, 6, 8, 10, or 24.')


def convert_to_timestamp(dt_info, unit='seconds'):
    if isinstance(dt_info, int):
        timestamp_sec = dt_info
    elif isinstance(dt_info, np.int64):
        timestamp_sec = int(dt_info)
    elif isinstance(dt_info, (datetime, pd.Timestamp)):
        timestamp_sec = int(dt_info.timestamp())
    elif isinstance(dt_info, str):
        dt = convert_str_to_dt(dt_info)
        timestamp_sec = int(dt.timestamp())
    else:
        raise TypeError('Can convert only datetime objects or numeric string objects of length, 6, 8, 10, or 24.')
    return timestamp_unit_change(timestamp_sec, unit)


def interval_to_minute(interval):
    time_dict = {'m': 1, 'h': 60, 'd': 1440}
    try:
        return int(interval)
    except:
        stride = int(interval[:-1])
        unit = interval[-1]
        return stride * time_dict[unit]
