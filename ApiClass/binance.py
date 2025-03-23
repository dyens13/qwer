from datetime import datetime, timedelta

import numpy as np
from ccxt.base.decimal_to_precision import decimal_to_precision

from ApiClass.base import ApiClass
# time and timestamp : milliseconds
# data : ascending order -> Oldest first, newest last
# rate limit : 2400/1m for public, 1200/1m for order, 300/10s for order
from utils.ftns_general import str_to_float

order_side = {1: 'BUY', -1: 'SELL'}

error_code = {
    '-2019': 'Margin is insufficient'
}
ONLY_FUTURES = ['DEFI', 'BTCDOM']


class ApiBinance(ApiClass):
    def __init__(self, acc_id=0, live=False):
        super().__init__('binance', acc_id)
        # self._set_ex_info()
        self.coin_list = {'SPOT': [], 'SWAP': []}
        self.ex_info = {'SPOT': {}, 'SWAP': {}}
        self.ticksize_precision = {'SPOT': {}, 'SWAP': {}}
        self.minqty_precision = {'SPOT': {}, 'SWAP': {}}
        self.maxqty_limit = {'SPOT': {}, 'SWAP': {}}
        self.maxqty_market = {'SPOT': {}, 'SWAP': {}}
        # self.maxNotionalValue = {'SPOT': {}, 'SWAP': {}}
        self._onboard = {'SWAP': {}}
        if live:
            self.posinfo = self.futures_posinfo()
            self._set_ex_info('SWAP')
            self._set_ex_info('SPOT')

    def _set_ex_info(self, market='SWAP', quote='USDT'):
        if market == 'SPOT':
            ftn = self.api.publicGetExchangeinfo
        elif market == 'SWAP':
            _posinfo = self.futures_posinfo('USDT')
            if quote.upper() in ['USDT', 'USDC']:
                ftn = self.api.fapiPublicGetExchangeInfo
            elif quote.upper() == 'USD':
                ftn = self.api.dapiPublicGetExchangeinfo
            else:
                raise ('wtf')
        api_out = self.Request(ftn, {}, retry=True)
        for info in api_out['symbols']:
            symbol = info['symbol']
            # if ('_' in info['symbol']) or (info['symbol'][-4:] != 'USDT'):
            #     continue
            # self.ex_info[market][info['pair']] = info
            if '_' in symbol:  # symbol : BTCUSDT_220624, pair : BTCUSDT
                continue
            if info['status'] != 'TRADING':
                continue
            self.ex_info[market][symbol] = info
            if info['quoteAsset'] == quote:
                self.coin_list[market].append(info['baseAsset'])
            else:
                continue
            if market == 'SWAP':
                self._onboard[market][symbol] = int(info['onboardDate']) / 1000
                self.maxNotionalValue[market][symbol] = _posinfo[symbol]['maxNotionalValue']
            for filter in info['filters']:
                # SWAP : PRICE_FILTER, LOT_SIZE, MARKET_LOT_SIZE, MAX_NUM_ORDERS, MAX_NUM_ALGO_ORDERS, MIN_NOTIONAL, PERCENT_PRICE
                # SPOT : *SWAP, ICEBERG_PARTS, TRAILING_DELTA
                if filter['filterType'] == 'PRICE_FILTER':
                    self.ticksize_precision[market][symbol] = float(filter['tickSize'])
                elif filter['filterType'] == 'LOT_SIZE':
                    self.minqty_precision[market][symbol] = float(filter['minQty'])
                    self.maxqty_limit[market][symbol] = float(filter['maxQty'])
                elif filter['filterType'] == 'MARKET_LOT_SIZE':
                    self.maxqty_market[market][symbol] = float(filter['maxQty'])
            # self.ticksize_precision[market][info['symbol']] = float(info['filters'][0]['tickSize'])
            # self.minqty_precision[market][info['symbol']] = float(info['filters'][2]['minQty'])
        return None

    def spot_get_accinfo(self):
        """
        [{'asset': 'BTC', 'free': '0.00000000', 'locked': '0.00000000'},
          {'asset': 'LTC', 'free': '0.00000000', 'locked': '0.00000000'},
          {'asset': 'ETH', 'free': '0.00000000', 'locked': '0.00000000'},
          {'asset': 'NEO', 'free': '0.00000000', 'locked': '0.00000000'},
        """
        api_out = self.Request(self.api.privateGetAccount, {})
        acc_info = str_to_float(api_out['balances'])
        # return str_to_float(api_out['balances'])

        output = {}
        for item in acc_info:
            output[item['asset'].upper()] = {'free': item['free'], 'locked': item['locked'], 'asset': item['free'] + item['locked']}
        return output

    def spot_ticker(self, coin=None, quote='USDT'):
        if coin:
            if coin == quote:
                return 1.0
            api_out = self.Request(self.api.publicGetTickerPrice, {'symbol': self.to_inst_id(coin, quote)})
            return float(api_out['price'])
        else:
            api_out = self.Request(self.api.publicGetTickerPrice, {})
            output = {}
            for item in api_out:
                output[item['symbol']] = float(item['price'])
            return output

    def spot_bookticker(self, coin=None, quote='USDT'):
        # {'symbol': 'ETHBTC',
        # 'bidPrice': '0.05131000', 'bidQty': '36.47980000', 'askPrice': '0.05132000', 'askQty': '19.17240000'}
        if coin:
            if coin == quote:
                return None
            api_out = self.Request(self.api.publicGetTickerBookTicker, {'symbol': self.to_inst_id(coin, quote)})
            return float(api_out['price'])
        else:
            api_out = str_to_float(self.Request(self.api.publicGetTickerBookTicker, {}))
            output = {}
            for item in api_out:
                output[item['symbol']] = item
            return output

    def spot_balance(self):
        spot_portfolio = self.spot_get_portfolio()
        quote = 'USDT'
        tickers = self.spot_ticker()
        balance = 0
        for coin in spot_portfolio.keys():
            if coin == quote:
                balance += spot_portfolio[coin]
            elif tickers.get(f'{coin}{quote}', 0):
                balance += spot_portfolio[coin] * tickers[f'{coin}{quote}']
            else:
                print(f'{coin} not in tickers')
        return balance

    def get_snapshot(self, market):
        ## daily!!
        # market : SPOT, FUTURES, MARGINS
        api_out = self.Request(self.api.sapiGetAccountSnapshot, {'type': market.upper()})
        output = None
        if market.upper() == 'SPOT':
            output = api_out['snapshotVos'][4]['data']
        elif market.upper() == 'FUTURES':
            output = api_out['snapshotVos'][4]['data']['assets']
        elif market.upper() == 'MARGINS':
            raise NotImplementedError('NOT Ready for get_snapshot(MARGINS)')
        return output

    def refresh_stat(self):
        self.posinfo = self.futures_posinfo()

    def get_univ(self, univsize=100, length=7, min_history=30):
        vol_dict = {}
        # only for swap
        if len(self.coin_list['SWAP']) == 0:
            self._set_ex_info()
        for coin in self.coin_list['SWAP']:
            futures_k = str_to_float(self.swap_kline(coin, '1d'))[::-1]
            if len(futures_k) < min_history:
                continue
            sum = 0
            sum_long = 0
            for i in range(1, 30):
                if i > len(futures_k):
                    break
                sum_long += futures_k[-i][7]
                if i <= length:
                    sum += futures_k[-i][7]
            if (sum_long < 100) or (futures_k[-1][7] < 100):
                continue
            vol_dict[coin] = sum
        sorted_vol = dict(sorted(vol_dict.items(), key=lambda item: item[1], reverse=True))
        univ_list = []
        for coin in sorted_vol.keys():
            univ_list.append(coin)
            if len(univ_list) >= univsize:
                break
        return univ_list

    @staticmethod
    def to_inst_id(coin, quote='USDT'):
        if coin.endswith(quote):
            return coin
        return f'{coin}{quote}'

    @staticmethod
    def from_symbol(symbol, quote='USDT'):
        return symbol.split(quote)[0], quote

    def internal_transfer(self, coin, amount, tftype):
        """
        type :
            1: transfer from spot account to USDT-Ⓜ futures account.
            2: transfer from USDT-Ⓜ futures account to spot account.
            3: transfer from spot account to COIN-Ⓜ futures account.
            4: transfer from COIN-Ⓜ futures account to spot account.
        """
        params = {'asset': coin, 'amount': amount, 'type': tftype}
        api_out = self.Request(self.api.sapiPostFuturesTransfer, params)
        return api_out

    def spot_kline(self, coin, interval, limit=1000, quote='USDT', startTime=None, endTime=None):
        # RATE LIMIT : 1
        # limit : 1 ~ 1000
        # limit, startTime, endTime
        # output :
        # open time, open, high, low, close, volume, close time, quote asset volume, number of trades
        if coin in ONLY_FUTURES:
            return self.swap_kline(coin, interval, limit, startTime, endTime)
        # params.update({'symbol': self.to_inst_id(coin), 'interval': PERIOD_DICT[interval], 'limit': limit})
        params = {'symbol': self.to_inst_id(coin, quote), 'interval': interval, 'limit': limit}
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
        api_out = self.Request(self.api.publicGetKlines, params)
        return api_out

    def spot_orderbook(self):
        raise NotImplementedError('spot orderbook not implemented yet')

    def spot_order(self, coin, side, orderType, quote='USDT', quantity=None):
        # fixme for limit order
        inst_id = self.to_inst_id(coin, quote)
        params = {'symbol': inst_id, 'side': order_side[side], 'type': orderType}
        if side > 0:
            rounding = 1
        else:
            rounding = 0
        if quantity:
            params['quantity'] = decimal_to_precision(quantity, rounding_mode=rounding,
                                                      precision=self.minqty_precision['SPOT'][inst_id],
                                                      counting_mode=4)
        api_out = self.Request(self.api.privatePostOrder, params)
        return api_out

    def spot_cancel_all(self):
        # fixme
        return None

    def swap_kline(self, coin, interval, limit=499, startTime=None, endTime=None):
        # RATE LIMIT
        # 1~99 : 1
        # 100~499 : 2
        # 500~999 : 5
        # 1000~1500 : 10
        ##################################
        # limit, startTime, endTime
        # output :
        # open time, open, high, low, close, volume, close time, quote asset volume, number of trades
        params = {'symbol': self.to_inst_id(coin), 'interval': interval, 'limit': limit}
        if startTime:
            params['startTime'] = startTime
        if endTime:
            params['endTime'] = endTime
        api_out = self.Request(self.api.fapiPublicGetKlines, params)
        return api_out

    def get_recent_klines(self, coin, interval, length, inst='spot', callfast=False):
        # limit 2400/1m
        if inst.lower() == 'spot':
            ftn = self.spot_kline
            unit_limit = 1000
        elif inst.lower() == 'swap':
            ftn = self.swap_kline
            if callfast:
                unit_limit = 1500
            else:
                unit_limit = 499
        else:
            raise ValueError('inst should be one of (spot, swap)')
        output = ftn(coin, interval, min(length, unit_limit))
        while length - unit_limit > 0:
            length -= unit_limit
            output = ftn(coin, interval, min(length, unit_limit), endTime=int(output[0][0]) - 1) + output
        return str_to_float(output)

    def futures_ticker(self, coin=None, quote='USDT'):
        if quote.upper() == 'USDT':
            ftn = self.api.fapiPublicGetTickerPrice
        elif quote.upper() == 'USD':
            ftn = self.api.dapiPublicGetTickerPrice
        else:
            raise ('wtf')
        if coin:
            api_out = self.Request(ftn, {'symbol': self.to_inst_id(coin, quote)})
            return float(api_out['price'])
        else:
            api_out = self.Request(ftn)
            output = {}
            for item in api_out:
                output[item['symbol']] = float(item['price'])
            return output

    def futures_bookticker(self, coin=None, quote='USDT'):
        # {'symbol': 'BTCUSDT',
        # 'bidPrice': '43798.30', 'bidQty': '10.147', 'askPrice': '43798.40', 'askQty': '0.406',
        # 'time': '1703082121326', 'lastUpdateId': '3687012275434'}
        if quote.upper() == 'USDT':
            ftn = self.api.fapiPublicGetTickerBookTicker
        elif quote.upper() == 'USD':
            ftn = self.api.dapiPublicGetTickerBookTicker
        else:
            raise ('wtf')
        if coin:
            api_out = self.Request(ftn, {'symbol': self.to_inst_id(coin, quote)})
            return float(api_out['price'])
        else:
            api_out = str_to_float(self.Request(ftn))
            output = {}
            for item in api_out:
                output[item['symbol']] = item
            return output

    def futures_ticker24h(self, coin=None, quote='USDT'):
        # {'symbol': 'XRPUSDT', 'priceChange': '-0.0035', 'priceChangePercent': '-0.721', 'weightedAvgPrice': '0.4833',
        # 'lastPrice': '0.4822', 'lastQty': '998.3', 'openPrice': '0.4857', 'highPrice': '0.4889', 'lowPrice': '0.4766',
        # 'volume': '550739773.2', 'quoteVolume': '266182745.5541', 'openTime': '1719920280000', 'closeTime': '1720006726269',
        # 'firstId': '1511329679', 'lastId': '1511822905', 'count': '493204'}
        if quote.upper() == 'USDT':
            ftn = self.api.fapipublicGetTicker24hr
        if coin:
            api_out = self.Request(ftn, {'symbol': self.to_inst_id(coin, quote)})
            return float(api_out['price'])
        else:
            api_out = str_to_float(self.Request(ftn))
            output = {}
            for item in api_out:
                output[item['symbol']] = item
            return output

    def futures_accinfo(self, quote='USDT'):
        """
        {'accountAlias': 'FzoCSgfWfWfWsRuX',
        'asset': 'USDT',
        'balance': 7777.13035624,
        'crossWalletBalance': 7777.13035624,
        'crossUnPnl': 0.0,
        'availableBalance': 5392.44208045,
        'maxWithdrawAmount': 5392.44208045,
        'marginAvailable': 1.0,
        'updateTime': 1721025510156.0}
        """
        if quote.upper() == 'USDT':
            ftn = self.api.fapiPrivateV2GetBalance
        elif quote.upper() == 'USD':
            ftn = self.api.dapiPrivateGetBalance
        else:
            raise ('wtf')
        # api_out = self.Request(self.api.fapiPrivateGetBalance, {})
        api_out = self.Request(ftn, {})
        output = {}
        for item in api_out:
            output[item['asset']] = item
            # asset : BNB, USDT, BUSD
            # {asset, balance, withdrawAvailable, updateTime}
        return str_to_float(output)

    def futures_posinfo(self, quote='USDT'):
        if quote.upper() == 'USDT':
            ftn = self.api.fapiPrivateV2GetPositionRisk
        elif quote.upper() == 'USD':
            ftn = self.api.dapiPrivateGetPositionrisk
        else:
            raise ('wtf')
        api_out = str_to_float(self.Request(ftn, {}))
        output = {}
        for pos in api_out:
            output[pos['symbol']] = pos
            """
            {'symbol': 'ALICEUSDT',
             'positionAmt': 16.8,
             'entryPrice': 11.952,
             'markPrice': 12.84597016,
             'unRealizedProfit': 15.01869868,
             'liquidationPrice': 11.9411144,
             'leverage': 20.0,
             'maxNotionalValue': 25000.0,
             'marginType': 'isolated',
             'isolatedMargin': 17.207684,
             'isAutoAddMargin': 'false',
             'positionSide': 'BOTH',
             'notional': 215.81229868,
             'isolatedWallet': 2.18898532,  # can be reduced
             'updateTime': 1620756838201.0}
             """
        return output

    def futures_portfolio(self, quote='USDT', update=False):
        output = {}
        if update:
            self.posinfo = self.futures_posinfo()
        for symbol in self.posinfo.keys():
            if self.posinfo[symbol]['positionAmt'] != 0:
                output[symbol] = self.posinfo[symbol]['positionAmt']
        return output

    def futures_uPNL(self, quote='USDT'):
        posinfo = self.futures_posinfo()
        uPNL = 0
        for inst, pos in posinfo.items():
            if inst.endswith(quote):
                uPNL += pos.get('unRealizedProfit', 0)
        return uPNL

    def futures_balance(self, coin='USDT', accinfo=None):
        if not accinfo:
            accinfo = self.futures_accinfo()
        output = {}
        output['balance'] = accinfo[coin]['balance'] + self.futures_uPNL(quote=coin)
        # output['balance'] = accinfo['USDT']['balance'] + self.futures_uPNL() + bnb_balance
        # output['available'] = accinfo['USDT']['withdrawAvailable']
        output['available'] = accinfo[coin]['availableBalance']
        output['usedMargin'] = accinfo[coin]['balance'] - output['available']
        return output

    def futures_agg_balance(self):
        # sum of bnb, usdt, busd in future?
        accinfo = self.futures_accinfo()
        bnb_balance = accinfo['BNB']['balance'] * self.futures_ticker('BNB', 'USDT')
        return bnb_balance + self.futures_balance(coin='USDT', accinfo=accinfo)['balance']

    def futures_margin_ratio(self):
        margin = self.futures_balance()
        margin_ratio = margin["usedMargin"] / (margin["usedMargin"] + margin["available"])
        return margin_ratio

    def futures_orderbook(self, coin=None, quote='USDT'):
        if quote.upper() == 'USDT':
            ftn = self.api.fapiPublicGetTickerBookTicker
        elif quote.upper() == 'USD':
            ftn = self.api.dapiPublicGetTickerBookTicker
        else:
            raise ('wtf')
        if coin:
            api_out = self.Request(ftn, {'symbol': self.to_inst_id(coin, quote)})
            # {'symbol': 'BTCUSDT', 'bidPrice': '66874.00', 'bidQty': '4.043', 'askPrice': '66874.10', 'askQty': '6.704', 'time': '1718111083587',
            return api_out
        else:
            api_out = self.Request(ftn)
            output = {}
            for item in api_out:
                output[item['symbol']] = str_to_float(item)
            return output

    def futures_order(self, coin, quote, side, orderType, quantity=None, price=None, stopPrice=None, timeInForce=None, closePosition=None, reduceOnly=None):
        if quote.upper() == 'USDT':
            ftn = self.api.fapiPrivatePostOrder
        elif quote.upper() == 'USD':
            ftn = self.api.dapiPrivatePostOrder
        else:
            raise ('wtf')
        """
        side : BUY or SELL
        !type   |   !mandatory
        limit   |   timeInForce, quantity, price
        Market  |   quantity
        STOP/TAKE_PROFIT    |   quantity, price, stopPrice
        STOP_MARKET/TAKE_PROFIT_MARKET  |    stopPrice
        TRAILING_STOP_MARKET    |   callbackRate
        timeInForce     |   GTC, IOC, FOK

        closePosition : true/false, with STOP/STOP_MARKET
        """
        symbol = self.to_inst_id(coin, quote)
        params = self.futures_order_params(symbol, side, orderType, quantity, price, stopPrice, timeInForce, closePosition, reduceOnly)
        api_out = self.Request(ftn, params)
        return api_out

    def futures_order_params(self, symbol, side, orderType, quantity, price=None, stopPrice=None, timeInForce=None, closePosition=None, reduceOnly=None):
        # print(params)
        # print(f'{symbol} {side} {quantity} {type}')
        # params.update({'symbol': symbol, 'side': order_side[side], 'type': orderType})
        params = {'symbol': symbol, 'side': order_side[side], 'type': orderType}
        if quantity:
            params['quantity'] = decimal_to_precision(quantity, rounding_mode=0,
                                                      precision=self.minqty_precision['SWAP'][symbol],
                                                      counting_mode=4)
        if price:
            params['price'] = decimal_to_precision(price, precision=self.ticksize_precision['SWAP'][symbol],
                                                   counting_mode=4)
        if stopPrice:
            params['stopPrice'] = decimal_to_precision(stopPrice, precision=self.ticksize_precision['SWAP'][symbol],
                                                       counting_mode=4)
        if timeInForce:
            params['timeInForce'] = timeInForce
        elif orderType == 'LIMIT':
            params['timeInForce'] = 'GTC'
        if closePosition:
            assert(closePosition in ['true', 'false'])
            params['closePosition'] = closePosition
        if reduceOnly:
            assert(reduceOnly in ['true', 'false'])
            params['reduceOnly'] = reduceOnly
        return params

    def futures_batch_orders(self, orders):
        # 5 batch orders at once
        if len(orders) == 0:
            print('= no orders to place')
            return None
        elif len(orders) > 5:
            raise ValueError(f'= too many orders! {len(orders)} orders input(max 5)')
        json_orders = [self.api.encode_uri_component(self.api.json(order), safe=",") for order in orders]
        api_input = '[' + ','.join(json_orders) + ']'
        params = {'batchOrders': api_input}
        api_outs = self.Request(self.api.fapiPrivatePostBatchOrders, params)
        # for api_out in api_outs:
        #     print(api_out)
        return api_outs

    def futures_check_order(self, coin, orderId):
        """
        {'orderId': '1071451577', 'symbol': 'OCEANUSDT', 'status': 'FILLED',
        'clientOrderId': 'tMwBtMMFXaaEhfu73tH7gk',
        'price': '0', 'avgPrice': '1.46766',
        'origQty': '137', 'executedQty': '137',  # in amount
        'cumQuote': '201.06942',  # in usd
        'timeInForce': 'GTC', 'type': 'MARKET', 'reduceOnly': False,
        'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH',
        'stopPrice': '0', 'workingType': 'CONTRACT_PRICE',
        'priceProtect': False, 'origType': 'MARKET',
        'time': '1620467109905', 'updateTime': '1620467109906'}
        """
        params = {'symbol': self.to_inst_id(coin), 'orderId': orderId}
        api_out = self.Request(self.api.fapiPrivateGetOrder, params)
        return api_out

    def futures_recent_orders(self, coin, quote='USDT', limit=None):
        """
        {'orderId': '1071451577', 'symbol': 'OCEANUSDT', 'status': 'FILLED',
        'clientOrderId': 'tMwBtMMFXaaEhfu73tH7gk',
        'price': '0', 'avgPrice': '1.46766',
        'origQty': '137', 'executedQty': '137',  # in amount
        'cumQuote': '201.06942',  # in usd
        'timeInForce': 'GTC', 'type': 'MARKET', 'reduceOnly': False,
        'closePosition': False, 'side': 'BUY', 'positionSide': 'BOTH',
        'stopPrice': '0', 'workingType': 'CONTRACT_PRICE',
        'priceProtect': False, 'origType': 'MARKET',
        'time': '1620467109905', 'updateTime': '1620467109906'}
        """
        params = {'symbol': self.to_inst_id(coin, quote)}
        if limit:
            params['limit'] = limit
        api_out = self.Request(self.api.fapiPrivateGetOrder, params)
        return api_out

    def futures_get_trades(self, coin=None, quote='USDT', history=None):
        # history in minute
        """
        {'symbol': 'XRPUSDT', 'id': '1278583712', 'orderId': '42982012169',
        'side': 'SELL', 'price': '0.4873', 'qty': '0.2', 'realizedPnl': '-0.00060306',
        'marginAsset': 'USDT', 'quoteQty': '0.09746', 'commission': '0.00000021',
        'commissionAsset': 'BNB', 'time': '1697018407459', 'positionSide': 'BOTH', 'maker': False, 'buyer': False}
        """
        params = {'limit': 1000}
        if coin:
            params['symbol'] = self.to_inst_id(coin, quote)
        api_out = str_to_float(self.Request(self.api.fapiPrivateGetUserTrades, params))
        agg_long_pnl = 0
        agg_short_pnl = 0
        agg_pnl = 0
        pnls = {}
        long_pnls = {}
        short_pnls = {}
        total_fee = {'USDT': 0, 'USDC': 0, 'BNB': 0}
        trades_dict = {}
        for fi in range(len(api_out)):
            trade_info = api_out[-fi]
            elapsed_time = datetime.now() - datetime.fromtimestamp((trade_info['time']) / 1000)
            if history:
                if elapsed_time.total_seconds() / 60 > history:
                    continue
            total_fee[trade_info['commissionAsset']] += trade_info['commission']
            symbol = trade_info['symbol']
            if symbol in pnls.keys():
                trades_dict[symbol].append(trade_info)
                pnls[symbol] += trade_info['realizedPnl']
                if trade_info['side'] == 'SELL':
                    long_pnls[symbol] += trade_info['realizedPnl']
                    agg_long_pnl += trade_info['realizedPnl']
                else:
                    short_pnls[symbol] += trade_info['realizedPnl']
                    agg_short_pnl += trade_info['realizedPnl']
            else:
                trades_dict[symbol] = [trade_info]
                pnls[symbol] = trade_info['realizedPnl']
                if trade_info['side'] == 'SELL':
                    long_pnls[symbol] = trade_info['realizedPnl']
                    short_pnls[symbol] = 0
                    agg_long_pnl += trade_info['realizedPnl']
                else:
                    long_pnls[symbol] = 0
                    short_pnls[symbol] = trade_info['realizedPnl']
                    agg_short_pnl += trade_info['realizedPnl']
        agg_pnl = agg_long_pnl + agg_short_pnl
        agg_info = {'all': agg_pnl, 'long': agg_long_pnl, 'short': agg_short_pnl}
        return {'fee': total_fee, 'pnls': pnls, 'long_pnls': long_pnls, 'short_pnls': short_pnls, 'trades': trades_dict, 'agg': agg_info}

    def futures_get_recent_trade_price(self, coin, side=None):
        _agg_trades = self.futures_get_trades(coin=coin)['trades']
        if f'{coin}USDT' not in _agg_trades.keys():
            return -1
        _trades = str_to_float(_agg_trades[f'{coin}USDT'])
        if len(_trades) == 0:
            return None
        # side : BUY or SELL
        if side not in ['BUY', 'SELL']:
            if side > 0:
                side = 'BUY'
            elif side < 0:
                side = 'SELL'
            else:
                raise ValueError('check get recent trade price!!!')
        _last_trade = None
        for _trade in _trades:
            if _trade['side'] == side:
                if _last_trade:
                    if _last_trade['time'] < _trade['time']:
                        _last_trade = _trade
                else:
                    _last_trade = _trade
        return _last_trade['price']

    def futures_set_leverage(self, coin, quote='USDT', inst=None, leverage=20):
        if not inst:
            inst = self.to_inst_id(coin, quote)
        # params = {'symbol': self.to_inst_id(coin, quote), 'leverage': leverage}
        params = {'symbol': inst, 'leverage': leverage}
        api_out = self.Request(self.api.fapiPrivatePostLeverage, params)
        if int(api_out.get('code', 0)) < 0:
            print(f'leverage change failed on {inst}')
        else:
            print(f'leverage change success on {inst}\tto {leverage}')
        return api_out

    def futures_set_leverage_all(self, quote='USDT', leverage=20):
        for coin in self.coin_list['SWAP']:
            # fixme -> for coin margin
            self.futures_set_leverage(coin, quote, leverage)
        return None

    def futures_set_margin_type(self, coin, quote='USDT', marginType='CROSSED'):
        # marginType : ISOLATED or CROSSED
        if marginType.upper() == 'CROSS':
            marginType = 'CROSSED'
        api_out = self.Request(self.api.fapiPrivatePostMarginType,
                               {'symbol': self.to_inst_id(coin, quote), 'marginType': marginType.upper()})
        if int(api_out.get('code', 0)) < 0:
            print(f'margin type change failed on {coin}')
        else:
            print(f'margin type change success on {coin}')
        return api_out

    def futures_set_margin_type_all(self, quote='USDT', marginType='CROSSED'):
        for coin in self.coin_list['SWAP']:
            _ = self.futures_set_margin_type(coin, quote, marginType)
        return None

    def futures_setting(self, coin, quote='USDT', leverage=20, marginType='CROSSED'):
        output = True
        try:
            self.futures_set_leverage(coin, quote, leverage=leverage)
            self.futures_set_margin_type(coin, quote, marginType=marginType)
        except Exception as e:
            print(e)
            output = False
        finally:
            return output

    def futures_adjust_margin(self, coin, amount, adjust_type=2):
        # adjust type : 1 - add, 2 - reduce
        params = {'symbol': self.to_inst_id(coin), 'type': adjust_type, 'amount': amount}
        api_out = self.Request(self.api.fapiPrivatePostPositionmargin, params)
        return api_out

    def futures_safe_order(self, coin, side, quantity=None, stop_price=None, take_price=None):
        # not implemented yet for limit order
        # orders = [{'symbol': self._to_inst_id(coin), 'side': order_side[side], 'type': 'MARKET', 'quantity': quantity}]
        orders = []
        inst_id = self.to_inst_id(coin)
        if stop_price:
            stop_price = decimal_to_precision(stop_price, precision=self.ticksize_precision['SWAP'][inst_id],
                                              counting_mode=4)
            orders.append(
                {'symbol': inst_id, 'side': order_side[-1 * side], 'type': 'STOP_MARKET', 'stopPrice': stop_price,
                 'closePosition': 'true'})
        if take_price:
            take_price = decimal_to_precision(take_price, precision=self.ticksize_precision['SWAP'][inst_id],
                                              counting_mode=4)
            orders.append({'symbol': inst_id, 'side': order_side[-1 * side], 'type': 'TAKE_PROFIT_MARKET',
                           'stopPrice': take_price, 'closePosition': 'true'})
        api_out = self.futures_batch_orders(orders)
        return api_out
        # for order in orders:
        #     print(order)
        #     api_out = self.Request(self.api.fapiPrivatePostOrder, order)
        # return api_out

    def futures_get_open_orders(self, coin, quote='USDT'):
        _inst_id = self.to_inst_id(coin, quote)
        api_out = self.Request(self.api.fapiprivateGetOpenorders, {'symbol': _inst_id})
        return api_out

    def futures_cancel_all_open_orders(self, coin, quote='USDT'):
        _inst_id = self.to_inst_id(coin, quote)
        print(f"cancel all orders on {_inst_id}")
        api_out = self.Request(self.api.fapiPrivateDeleteAllOpenOrders, {'symbol': _inst_id})
        return api_out

    def futures_cancel_all_countdown(self, coin, countdown=0):
        # countdown = 0 : cancel countdown
        # countdown 1000 for 1 seconds
        api_out = self.Request(self.api.fapiPrivatePostCountdowncancelall, {'symbol': self.to_inst_id(coin)})
        return api_out

    def refill_bnb(self, target_bnb_usd, both=False):
        price = self.futures_ticker('BNB')
        current_BNB = float(self.futures_accinfo()['BNB']['balance'])
        log = f'current BNB : {current_BNB:.3f} ({current_BNB * price:.2f} in USDT)'

        if current_BNB * price < target_bnb_usd / 2:
            self.internal_transfer('USDT', target_bnb_usd, tftype=2)
            self._set_ex_info()
            api_out = self.spot_order('BNB', 1, 'MARKET', quantity=0.99 * target_bnb_usd / price)
            transferBNB = self.spot_get_portfolio()['BNB']
            if both:
                transferBNB *= 0.5
            self.internal_transfer('BNB', transferBNB, 1)
            log += f'\n- {transferBNB:.3f} BNB (= {transferBNB * price:.2f} USDT) SEND TO FUTURES WALLET'
        print(log)
        return None

    def coin_to_futures(self, coin):
        spot_amt = self.spot_get_portfolio().get(coin, 0)
        if spot_amt > 0:
            if coin != 'USDT':
                price = self.spot_ticker(coin)
                if spot_amt * price < 5:
                    return None
                self._set_ex_info()
                api_out = self.spot_order(coin, -1, 'MARKET', quantity=spot_amt)
            move_USDT = self.spot_get_portfolio()['USDT']
        else:
            return None
        self.internal_transfer('USDT', move_USDT, tftype=1)
        log = f'\n- {move_USDT:.3f} USDT SEND TO FUTURES WALLET'
        print(log)
        return None

    def agg_balance(self):
        return self.spot_balance() + self.futures_agg_balance()

    def get_fee(self, length):
        _pnls = self.futures_get_trades(history=length)  # length in minute
        _total_fee = _pnls['fee']
        # _fee_in_usd = _total_fee['USDT'] + _total_fee['BNB'] * mdf.cps('BNB')
        if not self.tickers:
            self.set_tickers()
        elif 'BNB/USDT' not in self.tickers.keys():
            self.set_tickers()
        _fee_in_usd = _total_fee['USDT'] + _total_fee['BNB'] * self.tickers['BNB/USDT']['last']
        return _fee_in_usd

    def universal_transfer(self, coin, amt, fromType, toType, fromEmail=None, toEmail=None):
        # acc type : "SPOT","USDT_FUTURE","COIN_FUTURE","MARGIN"(Cross),"ISOLATED_MARGIN"
        params = {'asset': coin, 'amount': amt, 'fromAccountType': fromType, 'toAccountType': toType}
        if fromEmail:
            # for transfer from sub
            # none : master
            params['fromEmail'] = fromEmail
        if toEmail:
            # for transfer to sub
            # none : master
            params['toEmail'] = toEmail
        api_out = self.Request(self.api.sapiPostSubAccountUniversalTransfer, params)
        return api_out

    def transfer_to_sub(self, coin, amt, to_email):
        # on api of master_acc
        params = {'asset': coin, 'amount': amt}
        api_out = self.universal_transfer(coin, amt, )
        #fixme
        return None

    def transfer_to_master(self, coin, amt):
        # on api of sub_acc
        params = {'asset': coin, 'amount': amt}
        api_out = self.Request(self.api.sapiPostSubAccountTransferSubtomaster, params)
        return api_out

    def withdraw(self, coin, amt, address, network):
        params = {'coin': coin, 'amount': amt, 'address': address}
        if network:
            params['network'] = network
        api_out = self.Request(self.api.sapiPostCapitalWithdrawApply, params)
        return api_out

    def sub_transfer_history(self, tf_type=1):
        # tf_type : 1 in, 2 out
        # [{'counterParty': 'master', 'email': 'asdf@gmail.com', 'type': '1', 'asset': 'USDT',
        # 'qty': '10.00000000', 'time': '1725886709000', 'status': 'SUCCESS', 'tranId': '191072185489',
        # 'fromAccountType': 'SPOT', 'toAccountType': 'USDT_FUTURE'}]
        params = {'type': tf_type}
        api_out = self.Request(self.api.sapiGetSubAccountTransferSubUserHistory, params)
        return api_out


if __name__ == '__main__':
    api = ApiBinance()
    # test = binance_acc.futures_change_amt('BTC', 0.01)
