import ccxt
import pandas as pd
import datetime as dt
from dateutil import tz
import json


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    run_flag = config_system['run_flag']
    idle_stage = config_system['idle_stage']
    idle_loop = config_system['idle_loop']
    idle_rest = config_system['idle_rest']
    keys_path = config_system['keys_path']

    return run_flag, idle_stage, idle_loop, idle_rest, keys_path


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']
    budget = config_params['budget']
    grid = config_params['grid']
    value = config_params['value']
    start_safety = config_params['start_safety']
    circuit_limit = config_params['circuit_limit']
    decimal = config_params['decimal']

    return symbol, budget, grid, value, start_safety, circuit_limit, decimal


def get_time(timezone = 'Asia/Bangkok'):
    timestamp = dt.datetime.now(tz = tz.gettz(timezone))
    
    return timestamp


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


def update_budget(exchange, symbol, config_params_path):
    _, quote_currency = get_currency(symbol)

    with open(config_params_path) as config_file:
        config_params = json.load(config_file)
    
    balance = exchange.fetch_balance()
    amount = balance[quote_currency]['total']
    
    config_params['budget'] = amount

    with open(config_params_path, 'w') as config_file:
        json.dump(config_params, config_file, indent = 1)


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
    ask_price = ticker['ask']

    return ask_price


def get_last_loop_price(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)
    
    last_loop_price = last_loop_dict['price']

    return last_loop_price


def update_last_loop_price(exchange, symbol, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    last_price = get_last_price(exchange, symbol)
    last_loop_dict['price'] = last_price

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def get_cut_loss_flag(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)
    
    cut_loss_flag = last_loop_dict['cut_loss_flag']

    return cut_loss_flag


def update_cut_loss_flag(cut_loss_flag, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    last_loop_dict['cut_loss_flag'] = cut_loss_flag

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)
    

def get_balance(exchange, symbol, last_price):
    balance = exchange.fetch_balance()

    base_currency, quote_currency = get_currency(symbol)
    base_currency_val = balance[base_currency]['total'] * last_price
    quote_currency_val = balance[quote_currency]['total']
    total_val = base_currency_val + quote_currency_val
    
    return total_val