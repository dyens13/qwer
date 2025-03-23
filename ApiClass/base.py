import time

import ccxt
import numpy as np

from config.constants import api_yaml
from utils.ftns_general import load_yaml

max_retry = 5


class ApiClass:
    def __init__(self, api_name, acc_id=0):
        self.exchange = api_name
        _temp = load_yaml(api_yaml)[api_name][acc_id]
        self.api = getattr(ccxt, api_name)({
            'apiKey': _temp['api_key'],
            'secret': _temp['secret']
        })
        self.deposit = _temp.get('deposit', None)
        self.tickers = None

    def Request(self, req_fn, params={}, count=max_retry, retry=False):
        # acc_name = req_fn.__self__.name
        try:
            res = req_fn(params)
            time.sleep(0.005)
            return res
        except Exception as e:
            try:
                exchange = str(e).split(' ')[0]
                api_out = eval(str(e).split(exchange + ' ')[1])
            except Exception as e2:
                api_out = {'code': -1, 'msg': 'unknown error'}
            if count == max_retry:
                print('#' * 30)
                print('Request error!')
                print('- ftn : ' + str(req_fn))
                print('- params : ' + str(params))
                print(str(e))
                print('#' * 30)
            if retry and (count > 0):
                print(f'- retry, remaining: {count}')
                time.sleep(0.5)
                return self.Request(req_fn, params, count=count - 1)
            else:
                return api_out

    def set_tickers(self):
        self.tickers = self.api.fetch_tickers()

    def spot_get_portfolio(self):
        output = {}
        accinfo = self.spot_get_accinfo()
        for coin in accinfo.keys():
            if accinfo[coin]['asset'] > 0:
                output[coin] = accinfo[coin]['asset']
        return output

    def set_pos(self, coin, target_amt=None, delta_amt=None, orderType='MARKET', max_order_amt=None):
        if self.exchange in ['binance']:
            return self.futures_set_pos(coin, target_amt, delta_amt, orderType, max_order_amt)
        else:
            return self.spot_set_pos(coin, target_amt, delta_amt, orderType, max_order_amt)

    def spot_set_pos(self, coin, target_amt=None, delta_amt=None, orderType='MARKET', max_order_amt=None):
        def _current_amt():
            return self.spot_get_portfolio().get(coin, 0)
        output = []
        current_amt = _current_amt()
        print(f'== {coin} set pos \n=== {target_amt}\t{delta_amt}\t{max_order_amt}')
        if not delta_amt:
            if not target_amt:
                raise ValueError('one of target amt or delta amt must be given')

            delta_amt = target_amt - current_amt
        if delta_amt == 0:
            print('nothing to do!')
            return None
        if not target_amt:
            target_amt = current_amt + delta_amt
        # api_out = self.futures_order(coin, 'USDT', np.sign(delta_amt), type=type, quantity=abs(delta_amt), price=price)
        if len(self.coin_list['SPOT']) == 0:
            self._set_ex_info()
        # max_order_amt = min(max_order_amt, self.maxqty_market['SWAP'][self.to_inst_id(coin)] * 0.9)
        order_side = np.sign(delta_amt)
        # if not max_order_amt:
        #     max_order_amt = max_order_notional / order_price
        if orderType == 'MARKET':
            order_amt_all = abs(delta_amt)
            while order_amt_all > 0:
                if order_amt_all < max_order_amt * 1.1:
                    order_amt = order_amt_all
                else:
                    order_amt = min(order_amt_all, max_order_amt)
                print(f'= order on {coin}\t= side : {order_side}\t= amt : {order_amt}')
                api_out = self.spot_order(coin, order_side, orderType=orderType, quantity=order_amt)
                order_amt_all -= order_amt
        else:
            while _current_amt() > target_amt:
                order_amt_all = _current_amt() - target_amt
                if order_amt_all < max_order_amt * 1.1:
                    order_amt = order_amt_all
                else:
                    order_amt = min(order_amt_all, max_order_amt)
                orderbook = self.spot_orderbook(coin)
                orderPrice = orderbook['bidPrice'] if delta_amt > 0 else orderbook['askPrice']
                api_out = self.spot_order(coin, order_side, orderType=orderType, price=orderPrice, quantity=order_amt)
                time.sleep(0.5)
                output.append(api_out)
                print(api_out)
        return output

    def futures_set_pos(self, coin, target_amt=None, delta_amt=None, orderType='MARKET', max_order_amt=None):
        output = []
        print(f'== {coin} set pos \n=== {target_amt}\t{delta_amt}\t{max_order_amt}')
        if delta_amt is None:
            if target_amt is None:
                raise ValueError('one of target amt or delta amt must be given')
            current_pos = self.futures_posinfo()[self.to_inst_id(coin)]
            delta_amt = target_amt - float(current_pos['positionAmt'])
        if delta_amt == 0:
            print('nothing to do!')
            return None
        # api_out = self.futures_order(coin, 'USDT', np.sign(delta_amt), type=type, quantity=abs(delta_amt), price=price)
        if len(self.coin_list['SWAP']) == 0:
            self._set_ex_info()
        if max_order_amt:
            max_order_amt = min(max_order_amt, self.maxqty_market['SWAP'][self.to_inst_id(coin)] * 0.9)
        else:
            max_order_amt = self.maxqty_market['SWAP'][self.to_inst_id(coin)] * 0.9
        order_amt_all = abs(delta_amt)
        order_side = np.sign(delta_amt)
        while order_amt_all > 0:
            if order_amt_all < max_order_amt * 1.1:
                order_amt = order_amt_all
            else:
                order_amt = min(order_amt_all, max_order_amt)
            print(f'= order on {coin}\t= side : {order_side}\t= amt : {order_amt}')
            api_out = self.futures_order(coin, 'USDT', order_side, orderType=orderType, quantity=order_amt)
            order_amt_all -= order_amt
            time.sleep(0.5)
            output.append(api_out)
            print(api_out)
        return output
