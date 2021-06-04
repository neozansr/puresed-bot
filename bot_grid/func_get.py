import ccxt
import pandas as pd
import datetime as dt
from dateutil import tz
import json

from func_cal import cal_unrealised


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


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.ftx({'apiKey': keys_dict['apiKey'],
                         'secret': keys_dict['secret'],
                         'headers': {'FTX-SUBACCOUNT': keys_dict['subaccount']},
                         'enableRateLimit': True})

    return exchange


def convert_tz(utc):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = utc.replace(tzinfo = from_zone).astimezone(to_zone)
    
    return utc


def get_time(datetime_raw):
    datetime_str, _, us = datetime_raw.partition('.')
    datetime_utc = dt.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
    us = int(us.rstrip('Z'), 10)

    datetime_th = convert_tz(datetime_utc)
    
    return datetime_th + dt.timedelta(microseconds = us)


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

    print('Last price: {:.2f}'.format(last_price))
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


def print_pending_order(symbol, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    _, quote_currency = get_currency(symbol)
    
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    min_buy_price = min(open_buy_orders_df['price'], default = 0)
    max_buy_price = max(open_buy_orders_df['price'], default = 0)

    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    min_sell_price = min(open_sell_orders_df['price'], default = 0)
    max_sell_price = max(open_sell_orders_df['price'], default = 0)

    print('Min buy price: {:.2f} {}'.format(min_buy_price, quote_currency))
    print('Max buy price: {:.2f} {}'.format(max_buy_price, quote_currency))
    print('Min sell price: {:.2f} {}'.format(min_sell_price, quote_currency))
    print('Max sell price: {:.2f} {}'.format(max_sell_price, quote_currency))


def print_hold_assets(symbol, grid, last_price, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    unrealised_loss, n_open_sell_oders, amount, avg_price = cal_unrealised(grid, last_price, open_orders_df)

    assets_dict = {'datetime': dt.datetime.now(),
                   'last_price': last_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised_loss': unrealised_loss}

    assets_df = pd.DataFrame(assets_dict, index = [0])
    assets_df.to_csv('assets.csv', index = False)

    base_currency, quote_currency = get_currency(symbol)
    
    print('Hold {:.3f} {} with {} orders at {:.2f} {}'.format(amount, base_currency, n_open_sell_oders, avg_price, quote_currency))
    print('Unrealised: {:.2f} {}'.format(unrealised_loss, quote_currency))


def print_current_balance(exchange, symbol, last_price):
    balance = exchange.fetch_balance()
    base_currency, quote_currency = get_currency(symbol)
    
    try:
        base_currency_amount = balance[base_currency]['total']
        base_currency_value = last_price * base_currency_amount
    except KeyError:
        base_currency_value = 0
    
    try:
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0
    
    total_balance = base_currency_value + quote_currency_value

    print('Balance: {:.2f} {}'.format(total_balance, quote_currency))