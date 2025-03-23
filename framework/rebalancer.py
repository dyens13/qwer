import time
from datetime import datetime, timedelta

import numpy as np

from ApiClass.binance import ApiBinance
from utils.ftns_datetime import logging_dt
from utils.ftns_general import safe_ftn


def get_limit_price(side, orderbook, ticksize, depth=1):
    # orderbook : {'symbol': 'API3USDT',
    #           'bidPrice': 5.786,
    #           'bidQty': 66.0,
    #           'askPrice': 5.787,
    #           'askQty': 4.3,
    #           'time': 1646579970995.0}
    # depth < 0 -> between best bid/ask
    # depth 0 -> same as current best bid/ask
    # depth > 0 -> away from best bid/ask
    bidask = {-1: orderbook['askPrice'], 1: orderbook['bidPrice']}
    minmax = {-1: max, 1: min}
    # if depth == 0:
    #     return bidask[side]
    # elif depth < 0:
    #     return minmax[side](bidask[side] - side * depth * ticksize, bidask[-side] - side * ticksize)
    # elif depth > 0:
    #     return bidask[side] - side * depth * ticksize
    return bidask[side] - side * depth * ticksize


class Rebalancer:
    def __init__(self, api, MAXTIME=60, hump=0, max_order_usd=10000):
        self.api = api
        self.hump = hump
        self.tickers = self.api.futures_ticker()
        try:
            print(self.api.minqty_precision['SWAP']['BTCUSDT'])
        except Exception as e:
            print('== market info not found')
            self.api._set_ex_info()
        self.ticksize = self.api.ticksize_precision['SWAP']

        self.univ = None
        self.init_pos = None
        self.start_time = None
        self.stg = None
        self.MAXTIME = MAXTIME
        self.max_order_usd = max_order_usd

    def elapsed_time(self):
        return (datetime.now() - self.start_time).total_seconds()

    def check_delta_amt(self, symbol, delta_amt):
        output = True
        if np.isnan(delta_amt):
            print('#' * 50)
            print(f"stg amt for {symbol} is nan")
            print('#' * 50)
            output = False
        if abs(delta_amt) * self.tickers[symbol] <= max(5, self.hump):
            # print(f'too small amt to trade on {symbol}')
            output = False
        elif abs(delta_amt) < self.api.minqty_precision['SWAP'][symbol]:
            # print(f'too small amt to trade on {symbol}')
            output = False
        return output

    def sub_rebalancing_twap(self):
        split = 0.1
        time_ratio = self.elapsed_time() / 60 / self.MAXTIME
        for symbol in self.univ:
            safe_ftn(self.api.futures_cancel_all_open_orders, coin=symbol)
            # self.api.futures_cancel_all_open_orders(symbol)
        prev_pos = self.api.futures_portfolio(update=True)

        # get order params for batch order
        orders = []
        orderbook = self.api.futures_orderbook()
        print('=' * 50)
        print(f'{logging_dt()}\ttime ratio : {time_ratio:.2f}')
        check_finish = 0

        for symbol in self.univ:
            # check trade finished
            if self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0) == 0:
                continue
            # check nan, hump, minqty_precision
            if not self.check_delta_amt(symbol, self.stg.get(symbol, 0) - prev_pos.get(symbol, 0)):
                continue
            check_finish += 1
            ######################################################
            current_traded_ratio = (prev_pos.get(symbol, 0) - self.init_pos.get(symbol, 0)) / (self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0))
            if time_ratio > 0.8:
                target_pos = self.stg.get(symbol, 0)
                delta_amt = target_pos - prev_pos.get(symbol, 0)
                depth = 0
                if abs(delta_amt) * self.tickers[symbol] > 100:
                    depth = -1
                    delta_amt *= 1 / 2
                elif time_ratio > 0.9:
                    depth = -1
            else:
                target_pos = min(time_ratio + 0.1, 1) * (self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0)) + self.init_pos.get(symbol, 0)
                if not self.check_delta_amt(symbol, target_pos - self.stg.get(symbol, 0)):
                    target_pos = self.stg.get(symbol, 0)
                delta_amt = target_pos - prev_pos.get(symbol, 0)
                if not self.check_delta_amt(symbol, delta_amt):
                    continue
                ######################################################
                if current_traded_ratio < time_ratio - split:
                    depth = -2
                elif current_traded_ratio < time_ratio - split / 2:
                    depth = -1
                elif current_traded_ratio > time_ratio + split / 2:
                    depth = 1
                else:
                    depth = 0
                    # elif current_traded_ratio < time_ratio - 0.2:
                    #     depth = 0
                    # else:
                    #     depth = 1
                if abs(delta_amt) > abs(self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0)) * split:
                    delta_amt = (self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0)) * split
                if abs(delta_amt) * self.tickers[symbol] > self.max_order_usd:
                    delta_amt = self.max_order_usd / self.tickers[symbol]
                # depth = -1
            _order_log = f'{logging_dt()} {symbol:>13}({abs(current_traded_ratio):.2f})\t'
            _order_log += f'{prev_pos.get(symbol, 0):.2f} -> {target_pos:.2f}\t'
            _order_log += f'({self.init_pos.get(symbol, 0):.2f} => {self.stg.get(symbol, 0):.2f})\t'
            # _order_log += f'prev : {prev_pos.get(symbol, 0):.2f}\ttarget : {target_pos:.2f}\t'
            # _order_log += f'init : {self.init_pos.get(symbol, 0):.2f}\tstg : {self.stg.get(symbol, 0):.2f}\t'
            _order_log += f'depth : {depth}\torder_usd : {delta_amt * self.tickers[symbol]:.1f}'
            print(_order_log)
            order_price = get_limit_price(np.sign(delta_amt), orderbook[symbol], self.ticksize[symbol], depth=depth)
            order_param = self.api.futures_order_params(symbol, np.sign(delta_amt), orderType='LIMIT', quantity=abs(delta_amt), price=order_price)
            orders.append(order_param)
        ######################################################
        print(f'{logging_dt()}\tcheck finish : {check_finish}')
        # post batch order
        count = 0
        max_batch = 5
        while True:
            api_out = self.api.futures_batch_orders(orders[count * max_batch:(count + 1) * max_batch])
            # print('#' * 50)
            # print_pretty(orders[count * max_batch:(count + 1) * max_batch])
            # print_pretty(api_out)
            # print('#' * 50)
            count += 1
            if count * max_batch > len(orders):
                break

        return check_finish

    def sub_rebalancing_open(self):
        bdd_time = 10
        for symbol in self.univ:
            safe_ftn(self.api.futures_cancel_all_open_orders, coin=symbol)
            # self.api.futures_cancel_all_open_orders(symbol)
        prev_pos = self.api.futures_portfolio(update=True)
        # get need-to-trade amt
        delta_amt = {}
        for symbol in self.univ:
            delta_amt[symbol] = self.stg.get(symbol, 0) - prev_pos.get(symbol, 0)

        # get order params for batch order
        orders = []
        orderbook = self.api.futures_orderbook()
        if self.elapsed_time() / 60 > bdd_time * 1.5:
            depth = -2
        elif self.elapsed_time() / 60 > bdd_time:
            depth = -1
        else:
            depth = 0
        # generate order params
        for symbol in self.univ:
            if not self.check_delta_amt(symbol, delta_amt[symbol]):
                continue
            current_traded_ratio = (prev_pos.get(symbol, 0) - self.init_pos.get(symbol, 0)) / (self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0))

            _order_log = f'{logging_dt()} {symbol:>13}({abs(current_traded_ratio):.2f})\t'
            _order_log += f'{prev_pos.get(symbol, 0):.2f} -> {self.stg.get(symbol, 0):.2f}\t'
            _order_log += f'({self.init_pos.get(symbol, 0):.2f} => {self.stg.get(symbol, 0):.2f})\t'
            _order_log += f'depth : {depth}\torder_usd : {delta_amt[symbol] * self.tickers[symbol]:.1f}'
            print(_order_log)

            order_price = get_limit_price(np.sign(delta_amt[symbol]), orderbook[symbol], self.ticksize[symbol], depth=depth)
            order_param = self.api.futures_order_params(symbol, np.sign(delta_amt[symbol]), orderType='LIMIT', quantity=abs(delta_amt[symbol]), price=order_price)
            orders.append(order_param)
        print(f'{logging_dt()} - {len(orders)} orders will be submitted')
        # post batch order
        count = 0
        max_batch = 5
        while True:
            api_out = self.api.futures_batch_orders(orders[count * max_batch:(count + 1) * max_batch])
            if len(orders) < 5:
                print(api_out)
            # print('#' * 50)
            # print_pretty(orders[count * max_batch:(count + 1) * max_batch])
            # print_pretty(api_out)
            # print('#' * 50)
            count += 1
            if count * max_batch > len(orders):
                break

        return len(orders)

    def sub_rebalancing_market(self):
        for symbol in self.univ:
            safe_ftn(self.api.futures_cancel_all_open_orders, coin=symbol)
            # self.api.futures_cancel_all_open_orders(symbol)
        prev_pos = self.api.futures_portfolio(update=True)
        # get need-to-trade amt
        delta_amt = {}
        for symbol in self.univ:
            delta_amt[symbol] = self.stg.get(symbol, 0) - prev_pos.get(symbol, 0)

        # get order params for batch order
        orders = []
        orderbook = self.api.futures_orderbook()
        depth = -1
        # generate order params
        for symbol in self.univ:
            if not self.check_delta_amt(symbol, delta_amt[symbol]):
                continue
            current_traded_ratio = (prev_pos.get(symbol, 0) - self.init_pos.get(symbol, 0)) / (self.stg.get(symbol, 0) - self.init_pos.get(symbol, 0))

            _order_log = f'{logging_dt()} {symbol:>13}({abs(current_traded_ratio):.2f})\t'
            _order_log += f'{prev_pos.get(symbol, 0):.2f} -> {self.stg.get(symbol, 0):.2f}\t'
            _order_log += f'({self.init_pos.get(symbol, 0):.2f} => {self.stg.get(symbol, 0):.2f})\t'
            _order_log += f'depth : {depth}\torder_usd : {delta_amt[symbol] * self.tickers[symbol]:.1f}'
            print(_order_log)

            order_price = get_limit_price(np.sign(delta_amt[symbol]), orderbook[symbol], self.ticksize[symbol], depth=depth)
            order_param = self.api.futures_order_params(symbol, np.sign(delta_amt[symbol]), orderType='LIMIT', quantity=abs(delta_amt[symbol]), price=order_price)
            orders.append(order_param)
        print(f'{logging_dt()} - {len(orders)} orders will be submitted')
        # post batch order
        count = 0
        max_batch = 5
        while True:
            api_out = self.api.futures_batch_orders(orders[count * max_batch:(count + 1) * max_batch])
            if len(orders) < 5:
                print(api_out)
            # print('#' * 50)
            # print_pretty(orders[count * max_batch:(count + 1) * max_batch])
            # print_pretty(api_out)
            # print('#' * 50)
            count += 1
            if count * max_batch > len(orders):
                break

        return len(orders)

    def rebalancing(self, stg_in_symbol, method='open'):
        print(f'{logging_dt()}\tstart rebalancing for {self.MAXTIME:.2f} minutes!')
        trade_method = {
            'open': self.sub_rebalancing_open,
            'twap': self.sub_rebalancing_twap,
            'market': self.sub_rebalancing_market
        }
        if method not in trade_method.keys():
            raise ValueError(f'method must be one of {trade_method.keys()}')

        self.init_pos = self.api.futures_portfolio()

        # check excluded coin
        for symbol in self.init_pos.keys():
            if symbol not in stg_in_symbol.keys():
                stg_in_symbol[symbol] = 0
        self.stg = stg_in_symbol
        self.univ = stg_in_symbol.keys()
        self.start_time = datetime.now()

        end_time = self.start_time + timedelta(minutes=self.MAXTIME)
        count = 0
        sleep_time = 10

        while True:
            print(f'{logging_dt()}\t=========== {count}-th order loop start ==========')
            order_count = trade_method[method]()
            if order_count == 0:
                break
            elif datetime.now() > end_time:
                print('too long rebalancing')
                break
            count += 1
            time.sleep(sleep_time)

        new_posinfo = self.api.futures_posinfo()
        traded_value = 0
        for symbol in self.univ:
            traded_value += new_posinfo[symbol]['markPrice'] * abs(
                new_posinfo[symbol]['positionAmt'] - self.init_pos.get(symbol, 0))
        return traded_value


if __name__ == '__main__':
    api = ApiBinance('binance', live=True)
    reb = Rebalancer(api, MAXTIME=30)
    # stg = {'BTCUSDT': 0.1, 'ETHUSDT': -10}  # in amt
    # test = reb.rebalancing(stg, method='twap')
