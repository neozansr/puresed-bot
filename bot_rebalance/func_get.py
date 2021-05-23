import random
import ccxt
import json


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    loop_flag = config_system['loop_flag']
    idle_stage = config_system['idle_stage']
    min_idle = config_system['min_idle']
    max_idle = config_system['max_idle']

    return loop_flag, idle_stage, min_idle, max_idle


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']
    fix_value = config_params['fix_value']
    min_value = config_params['min_value']

    return symbol, fix_value, min_value


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.kucoin({'apiKey': keys_dict['apiKey'],
                            'secret': keys_dict['secret'],
                            'password': keys_dict['password'],
                            'enableRateLimit': True})

    return exchange


def get_random(min_idle, max_idle):
    idle_loop = random.randint(min_idle, max_idle)
    
    print('Wait {} seconds'.format(idle_loop))
    return idle_loop
    

def get_latest_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    latest_price = ticker['last']

    print('latest_price: {}'.format(latest_price))
    return latest_price


def get_coin_name(symbol):
    trade_coin = symbol.split('/')[0]
    ref_coin = symbol.split('/')[1]

    return trade_coin, ref_coin
    

def get_current_value(exchange, symbol, latest_price):
    balance = exchange.fetch_balance()
    trade_coin, ref_coin = get_coin_name(symbol)
    
    try:
        amount = balance[trade_coin]['total']
        current_value = latest_price * amount
    except KeyError:
        current_value = 0

    print('Current value: {} {}'.format(current_value, ref_coin))
    return current_value


def print_current_balance(exchange, symbol, latest_price):
    balance = exchange.fetch_balance()
    trade_coin, ref_coin = get_coin_name(symbol)
    
    try:
        trade_coin_amount = balance[trade_coin]['total']
        trade_coin_value = latest_price * trade_coin_amount
    except KeyError:
        trade_coin_value = 0

    try:    
        ref_coin_value = balance[ref_coin]['total']
    except KeyError:
        ref_coin_value = 0

    total_balance = trade_coin_value + ref_coin_value

    print('Current balance: {} {}'.format(total_balance, ref_coin))