import re
import sys
import traceback

import yaml


class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def safe_ftn(ftn, **kwargs):
    # usage:
    # safe_ftn(is_zero, num='test')
    try:
        output = ftn(**kwargs)
    except Exception as e:
        print('='*100)
        print(f'=== Error on {str(ftn)}')
        print(e)
        print(sys.exc_info())
        print(traceback.format_exc())
        print('='*100)
        output = None
    return output


def safe_retry(ftn, *args, **kwargs):
    retry = 5
    sleep = 5

    def try_func():
        try:
            res = ftn(*args, **kwargs)
            return res
        except Exception as e:
            print('*' * 50)
            print(e)
            print(sys.exc_info())
            print(traceback.format_exc())
            print('*' * 50)
            return e

    res = try_func()
    import time
    while (retry > 0) and isinstance(res, Exception):
        res = try_func()
        retry -= 1
        time.sleep(sleep)

    if isinstance(res, Exception):
        raise res
    else:
        return res


def save_yaml(yaml_path, content):
    with open(r'%s' % yaml_path, 'w') as file:
        doc = yaml.dump(content, file)
    return None


def load_yaml(yaml_path):
    def _load_yaml():
        try:
            f = open(yaml_path, 'r')
        except Exception as e:
            content = {}
            print('#' * 100)
            print('yaml load error, update with empty dictionary!')
            print('#' * 100)
            save_yaml(yaml_path, content)
        else:
            content = yaml.load(f, Loader=yaml.Loader)
            f.close()
        return content

    content = safe_retry(_load_yaml)
    return content


def minmax(side):
    if side > 0:
        return max
    elif side < 0:
        return min
    else:
        raise ValueError('input must not be 0')


def gmax(*args):
    output = 0
    for arg in args:
        if arg:
            if output < arg:
                output = arg
    return output


def gmin(*args):
    output = 0
    for arg in args:
        if arg:
            if output > arg:
                output = arg
    return output


def is_zero(num):
    bdd = 1e-10
    if abs(num) < bdd:
        return True
    else:
        return False


def is_similar(num1, num0, bdd=0.001):
    return abs(num1 - num0) / num0 < bdd


def is_same(num1, num2):
    return is_zero(abs(num1 - num2))


def list_str_to_float(input_str):
    for index in range(len(input_str)):
        if isinstance(input_str[index], list):
            input_str[index] = list_str_to_float(input_str[index])
        elif isinstance(input_str[index], dict):
            input_str[index] = dict_str_to_float(input_str[index])
        else:
            try:
                input_str[index] = float(input_str[index])
            except Exception as e:
                pass
    return input_str


def dict_str_to_float(input_str):
    for key in input_str.keys():
        if isinstance(input_str[key], dict):
            input_str[key] = dict_str_to_float(input_str[key])
        elif isinstance(input_str[key], list):
            input_str[key] = list_str_to_float(input_str[key])
        else:
            try:
                input_str[key] = float(input_str[key])
            except Exception as e:
                pass
    return input_str


def str_to_float(input_str):
    if isinstance(input_str, list):
        return list_str_to_float(input_str)
    elif isinstance(input_str, dict):
        return dict_str_to_float(input_str)


def dict_to_str(dict):
    dict_list = [f'{key} : {dict[key]}' for key in dict.keys()]
    return ', '.join(dict_list)


def dict_to_float(input_dict):
    input_dict = dict(input_dict)
    for k, v in input_dict.items():
        input_dict[k] = float(v)
    return input_dict


def print_pretty(to_print, pre='= '):
    text = ''
    if isinstance(to_print, list):
        for item in to_print:
            text += f'{pre}{str(item)}\n'
    elif isinstance(to_print, dict):
        for k, v in to_print.items():
            text += f'{pre}{str(k)} : {str(v)}\n'
    print(text[:-1])
    return text[:-1]


def split_str_num(s):
    for n, c in enumerate(s[::-1]):
        if not c.isdigit():
            if n == 0:
                return s, None
            return s[:-n], int(s[-n:])


def dict_key(mydict, myvalue):
    return list(mydict.keys())[list(mydict.values()).index(myvalue)]


def expr_replace(expr, df_name):
    def split_into_words(line):
        import re
        word_regex_improved = r"(\w[\w']*\w|\w)"
        word_matcher = re.compile(word_regex_improved)
        return word_matcher.findall(line)
    # fields = split_into_words(expr)
    fields = re.findall('([a-zA-Z][a-zA-Z0-9]+(?:_[a-zA-Z0-9]+)*)', expr)
    for field in fields:
        expr = expr.replace(field, f'{df_name}.{field}')
        # fixme!
        expr = expr.replace('_mdf.', '_')
        expr = expr.replace('xmdf.', '')
    while 'mdf.mdf' in expr:
        expr = expr.replace('mdf.mdf', 'mdf')
    return expr


def expr_replace_simple(expr, df_name):
    return expr.replace('#', 'mdf.')
