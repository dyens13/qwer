----------------------------
- my referral link : https://accounts.binance.com/register?ref=36504442
- without referral -> pay $4 to Binance as fee for trading $10000
- with referral -> same $4 fee for trading $10000 but $3.2 for Binance and $0.8 for me for 1 year
----------------------------
- how to start
- 1 : install ccxt (choose 1-1 or 1-2)
- 1-1 : pip install ccxt  (not recommended since too heavy)
- 1-2 : pull my ccxt (https://github.com/dyens13/ccxt) and install manually

- 2 : go to mqf folder
- 3 : pip install -r requirements.txt

- 4 : make api key on binance, should set ip whitelist to avoid api key expiration
- 5 : on /config folder, change file name from api_key.yaml.example to api_key.yaml
- 6 : enter your api info on /config/api_key.yaml
- 7 : make telegram bot(read caption on utils/ftns_telegram.py)
----------------------------
- 8 : get used to mdf framework (framework/mdf.py)
- 9 : check ApiClass/binance.py and read what functions we have
----------------------------
- 10-0 : set leverage and margin type by set_ex.py
- 10-1 : check ex_simple.py for simple buy and sell
- 10-2 : checkex_alpha.py for simple statistical arbitrage(= alpha research)
- 11 : you may use framework/rebalancer for set positions
----------------------------