from ApiClass.load_api import load_api
from framework.mdf import MDF
from utils.ftns_datetime import interval_to_minute
from utils.ftns_telegram import send_telegram

univ = ['BTC', 'ETH', 'BNB']
mdf = MDF(univ, ['swap'], stride='1h', history=500, quote='USDT', exchange='binance')
api = load_api('binance', acc_id=0)
balance = api.agg_balance()
# api.coin_to_futures('USDT')  # transfer USDT from spot wallet to futures wallet

unit_position = 100
leverage = 10
marginType = 'isolated'  ## isolated or cross

######################################################################################
# set leverage and cross/isolated setting first
######################################################################################
all_posinfo = api.futures_posinfo()
for coin in univ:
    pos_info = all_posinfo[f'{coin}USDT']
    current_pos = pos_info['notional']
    if (pos_info['leverage'] != leverage) or (pos_info['marginType'] != marginType):
        if current_pos == 0:  # if not, below function won't work
            is_available = api.futures_setting(coin, leverage=leverage, marginType=marginType)

######################################################################################
### set long position if recent returns high
######################################################################################
bot_log = f"current balance : {balance:.2f}"
for coin in univ:
    ret_zsc = mdf.tsZscore60_returns.get(coin)[-1]
    if ret_zsc > 2:
        cps = mdf.close.get(coin)[-1]
        order_amt = unit_position / cps
        # buy 100 usd position via market order
        api.futures_order(coin, 'USDT', 1, 'MARKET', quantity=order_amt)
        bot_log += f"\nenter {coin}"
        # api.futures_order(coin, 'USDT', 1, 'LIMIT', price=cps, quantity=order_amt)

######################################################################################
### set stop_market order for safety
######################################################################################
all_posinfo = api.futures_posinfo()
for coin in univ:
    pos_info = all_posinfo[f'{coin}USDT']
    current_pos = pos_info['notional']
    if current_pos > 0:
        liqPrice = pos_info['liquidationPrice']
        # set stop_market order on liqPrice * 1.05
        # stop market order : trigger market order if market price touches stopPrice
        ## you may set more buy order before liqPrice to safe your position (this way will increase your mdd)
        api_out = api.futures_order(coin, 'USDT', -1, type='STOP_MARKET', stopPrice=liqPrice * 1.05, closePosition='true')
    elif current_pos < 0:
        pass
######################################################################################
### check recent 1 hour pnl stat
######################################################################################
pnl_log = ''
pnls = api.futures_get_trades(history=interval_to_minute('1h'))
for inst in pnls['pnls'].keys():
    pnl = pnls['pnls'].get(inst)
    _coin = inst.split('USDT')[0]
    if pnl != 0:
        pnl_log += f"--- {_coin} closed! (pnl : {pnl:.2f})\n"

######################################################################################
# send telegram
######################################################################################
if len(pnl_log) + len(pnl_log) > 0:
    send_telegram(bot_log + pnl_log)
