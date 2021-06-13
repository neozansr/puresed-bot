import ccxt
import datetime as dt
from dateutil import tz
import random
import json


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    run_flag = config_system['run_flag']
    idle_stage = config_system['idle_stage']
    keys_path = config_system['keys_path']

    return run_flag, idle_stage, keys_path


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']
    fix_value = config_params['fix_value']
    min_value = config_params['min_value']

    return symbol, fix_value, min_value


def get_time(timezone = 'Asia/Bangkok'):
    timestamp = dt.datetime.now(tz = tz.gettz(timezone))
    
    return timestamp


def get_date(timezone = 'Asia/Bangkok'):
    timestamp = dt.datetime.now(tz = tz.gettz(timezone))
    date = timestamp.date()
    
    return date


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.ftx({'apiKey': keys_dict['apiKey'],
                         'secret': keys_dict['secret'],
                         'headers': {'FTX-SUBACCOUNT': keys_dict['subaccount']},
                         'enableRateLimit': True})

    return exchange
    

def get_currency(symbol):
    base_currency = symbol.split('/')[0]
    quote_currency = symbol.split('/')[1]

    return base_currency, quote_currency


def get_last_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    last_price = ticker['last']

    _, quote_currency = get_currency(symbol)
    print('Last price: {:.2f} {}'.format(last_price, quote_currency))
    return last_price


def get_bid_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    bid_price = ticker['bid']

    return bid_price


def get_ask_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    ask_price = ticker['bid']

    return ask_price
    

def get_current_value(exchange, symbol, last_price):
    balance = exchange.fetch_balance()
    base_currency, quote_currency = get_currency(symbol)
    
    try:
        amount = balance[base_currency]['total']
        current_value = last_price * amount
    except KeyError:
        current_value = 0

    print('Current value: {:.2f} {}'.format(current_value, quote_currency))
    return current_value


def get_balance(exchange, symbol, last_price):
    balance = exchange.fetch_balance()
    base_currency, quote_currency = get_currency(symbol)
    
    try:
        base_currency_amount = balance[base_currency]['total']
    except KeyError:
        base_currency_amount = 0

    base_currency_value = last_price * base_currency_amount

    try:    
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0
    
    balance = base_currency_value + quote_currency_value
    
    return balance


def gen_series(n = 16):
    # haxanacci
    def hexa(n) :
        if n in range(6):
            return 0
        elif n == 6:
            return 1
        else :
            return (hexa(n - 1) +
                    hexa(n - 2) +
                    hexa(n - 3) +
                    hexa(n - 4) +
                    hexa(n - 5) +
                    hexa(n - 6))
    
    series = []
    for i in range(6, n):
        series.append(hexa(i))
        
    return series


def get_idle_loop(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    n_loop = last_loop_dict['n_loop']
    series = gen_series()
    idle_loop = series[n_loop]

    update_n_loop(n_loop, last_loop_dict, last_loop_dict, last_loop_path)
    
    return idle_loop


def update_n_loop(n_loop, series, last_loop_dict, last_loop_path):
    n_loop += 1
    if n_loop >= len(series):
        n_loop = 0

    last_loop_dict['n_loop'] = n_loop

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def reset_n_loop(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    last_loop_dict['n_loop'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def append_cash_flow_df(cur_date, balance, cash_flow, cash, cash_flow_df, cash_flow_df_path):
    cash_flow_df.loc[len(cash_flow_df)] = [cur_date, balance, cash_flow, cash]
    cash_flow_df.to_csv(cash_flow_df_path, index = False)