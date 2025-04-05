from ApiClass.binance import ApiBinance

api = ApiBinance(acc_id=0, live=True)

api.futures_set_leverage_all(leverage=5)
# api.set_futures_leverage('BTC', leverage=5)
api.futures_set_margin_type_all(marginType='ISOLATED')  # CROSSED or ISOLATED

# always keep in mind that liquidation price is depending on instrument's liquidity
# for example, 20 leverage BTC pos will be liquidated for about 4.8% loss but for 3.5% loss for small cap coin
# you can check the liquidation price of your position by api.futures_posinfo()[inst]
