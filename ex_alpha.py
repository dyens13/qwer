from framework.mqf import Simulation
from framework.operations import Operations
from utils.ftns_general import expr_replace_simple

univ = ['BTC', 'ETH', 'ETC', 'XRP', 'EOS', 'BNB', 'DOGE']
# univ = ['BTC', 'ETH', 'TRUMP', 'SOL', 'PNUT', 'NEIRO', 'NOT', 'ENA', 'BOME', 'KAITO', 'ORDI', 'MOODENG', 'DOGE', 'AVAX', 'ACT', 'TIA', 'XRP', 'USUAL', 'MOVE', 'DOGS', 'RARE', '1000BONK', 'LINK', 'IO', 'MEME', 'VINE', 'IP', 'GOAT', '1000LUNC', 'TST', 'BERA', 'PENGU', 'ZRO', 'MANTA', '1000SATS', 'BLUR', 'ETHFI', 'BNB', 'BB', 'BIGTIME', 'GAS', 'FARTCOIN', 'XAI', 'AI16Z', 'ADA', 'JTO', 'SUN', 'PIXEL', 'MELANIA', 'NEIROETH', '1MBABYDOGE', 'THE', 'DYDX', 'ALT', 'AIXBT', 'TROY', 'ME', 'VANA', 'HIVE', 'LAYER', 'SWARMS', 'MOCA', 'W', 'VIRTUAL', 'STRK', 'EIGEN', 'WLD', 'SEI', 'OP', 'AEVO', 'CATI', 'ARC', 'LISTA', '1000PEPE', 'GMT', 'SAGA', 'ARB', 'VVV', 'RUNE', 'DYM', 'LTC', 'DRIFT', 'ACE', 'GALA', 'DOT', 'TRB', 'DIA', 'ZK', 'FIL', 'UXLINK', '1000SHIB', 'NEAR', 'ETC', 'BIO', 'ATOM', 'JUP', 'BCH', 'MYRO', 'HMSTR']

constants = {
    'univ': univ,
    'history': 10000,
    'insts': ['spot', 'swap'],
    'stride': '1h',
    'source': 'api'
}

stg_info = {
    'constants': constants
}

s = Simulation.from_dict(stg_info)

mdf = s.df

# length = 12
# expr = f'(#MA{length}_xclose - #MA{length}_close) / #close'
expr = f'#returns24'
expr = f'#MA120_close - #MA24_close'

usd_filter = 1 * 1e7
####################################################################################
pos = eval(expr_replace_simple(expr, 'mdf'))
pos = Operations.filterExpr(pos, mdf, f'#usd{7 * mdf.day} > {7 * usd_filter}', interval=24)
# pos = Operations.filterExpr(pos, mdf, f'tsStd24_returns1 < 0.1')
####################################################################################
pos = Operations.normalize(pos, method=1)
pos = Operations.filterCount(pos, count=5)
pos = Operations.neut(pos)
pos = Operations.scale(pos, max_mode=False)
pos = Operations.nanToZero(pos)
####################################################################################
stat, simRet = s.simul(pos, buying_price='open', verbose=True)
