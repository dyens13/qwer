import math
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Context
from functools import partialmethod

import numpy as np


class Decimal_op:
    ctx = Context()

    # 20 digits should be enough for everyone :D
    ctx.prec = 20

    @staticmethod
    def add(flt1, flt2):
        return float(Decimal(str(flt1)) + Decimal(str(flt2)))

    @staticmethod
    def sub(flt1, flt2):
        return float(Decimal(str(flt1)) - Decimal(str(flt2)))

    @staticmethod
    def diff(flt1, flt2):
        return float((Decimal(str(flt1)) - Decimal(str(flt2))).copy_abs())

    @staticmethod
    def multiply(flt1, flt2):
        return float(Decimal(str(flt1)) * Decimal(str(flt2)))

    @staticmethod
    def div(flt1, flt2):
        return float(Decimal(str(flt1)) / Decimal(str(flt2)))

    @staticmethod
    def rounding_with_unit(flt, unit, rounding):
        # rounding : ROUND_CEILING, ROUND_FLOOR, ROUND_UP, ROUND_DOWN,
        #   ROUND_HALF_UP, ROUND_HALF_DOWN, ROUND_HALF_EVEN, ROUND_05UP
        _flt = str(flt)
        _unit = str(unit)

        decimal_flt = Decimal(_flt)
        decimal_unit = Decimal(_unit).copy_abs()

        return float((decimal_flt.copy_abs() / decimal_unit).quantize(Decimal('1.'), rounding=rounding).copy_sign(
            decimal_flt) * decimal_unit)

    @classmethod
    def float_to_str(cls, f):
        """
        Convert the given float to a string,
        without resorting to scientific notation
        """
        d1 = cls.ctx.create_decimal(repr(f))
        return format(d1, 'f')

    round = partialmethod(rounding_with_unit, rounding=ROUND_HALF_UP)
    ceil = partialmethod(rounding_with_unit, rounding=ROUND_CEILING)
    floor = partialmethod(rounding_with_unit, rounding=ROUND_FLOOR)


def str_to_float(input_str, decimal=7):
    return Decimal_op.floor(input_str, 10 ** (-decimal))


def float_to_str(input_float, digits=None):
    output = ''
    if digits is None:
        digits = math.log(np.nanmean(abs(input_float)) + 0.1, 10)
    if digits < 4:
        unit = [1, '']
    elif digits < 7:
        unit = [10 ** 3, 'k']
    else:
        unit = [10 ** 6, 'm']

    output += str(np.round(input_float / unit[0], 1)) + unit[1]
    return output


def vector_to_str(input_vector):
    output = '[ '
    digits = math.log(np.nanmean(abs(input_vector)) + 0.1, 10)
    if digits < 4:
        unit = [1, '']
    elif digits < 7:
        unit = [10 ** 3, 'k']
    else:
        unit = [10 ** 6, 'm']

    for i in range(len(input_vector)):
        output += str((input_vector[i] / unit[0]).round(1)) + unit[1]
        output += '  '
    output = output[:-2] + ' ]'
    return output


def array_to_str(input_array):
    output = ""
    for index in range(len(input_array)):
        output += vector_to_str(input_array[index])
        output += "\n"
    return output[:-1]
