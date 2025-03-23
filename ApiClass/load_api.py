from ApiClass.binance import ApiBinance


def load_api(exchange, **kwargs):
    if exchange == 'binance':
        return ApiBinance(**kwargs)


if __name__ == '__main__':
    bapi = load_api('binance')
