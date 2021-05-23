import pandas as pd
import datetime as dt
from dateutil import tz
import json
import ccxt

from func_cal import cal_unrealised


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    loop_flag = config_system['loop_flag']
    idle_stage = config_system['idle_stage']
    idle_loop = config_system['idle_loop']

    return loop_flag, idle_stage, idle_loop


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']
    budget = config_params['budget']
    grid = config_params['grid']
    value = config_params['value']
    min_price = config_params['min_price']
    max_price = config_params['max_price']
    fee_percent = config_params['fee_percent']
    start_market = config_params['start_market']

    return symbol, budget, grid, value, min_price, max_price, fee_percent, start_market


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.kucoin({'apiKey': keys_dict['apiKey'],
                            'secret': keys_dict['secret'],
                            'password': keys_dict['password'],
                            'enableRateLimit': True})

    return exchange


def get_latest_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    latest_price = ticker['last']

    print('latest_price: {}'.format(latest_price))
    return latest_price


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


def get_coin_name(symbol):
    trade_coin = symbol.split('/')[0]
    ref_coin = symbol.split('/')[1]

    return trade_coin, ref_coin


def get_balance(exchange, symbol, latest_price):
    balance = exchange.fetch_balance()

    trade_coin, ref_coin = get_coin_name(symbol)
    trade_coin_val = balance[trade_coin]['total'] * latest_price
    ref_coin_val = balance[ref_coin]['total']
    total_val = trade_coin_val + ref_coin_val
    
    return total_val


def print_hold_assets(open_orders_df, symbol, grid, latest_price):
    unrealised_loss, n_open_sell_oders, amount, avg_price = cal_unrealised(grid, latest_price, open_orders_df)

    assets_dict = {'datetime': dt.datetime.now(),
                   'latest_price': latest_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised_loss': unrealised_loss}

    assets_df = pd.DataFrame(assets_dict, index = [0])
    assets_df.to_csv('assets.csv', index = False)

    trade_coin, ref_coin = get_coin_name(symbol)
    message = 'hold {} {} with {} orders at {} {}\n{} {} unrealised'.format(amount, trade_coin, n_open_sell_oders, avg_price, ref_coin, unrealised_loss, ref_coin)
    print(message)


def print_current_balance(exchange, symbol, latest_price):
    balance = exchange.fetch_balance()
    trade_coin, ref_coin = get_coin_name(symbol)
    
    trade_coin_amount = balance[trade_coin]['total']
    trade_coin_value = latest_price * trade_coin_amount
    ref_coin_value = balance[ref_coin]['total']
    total_balance = trade_coin_value + ref_coin_value

    print('Current balance: {} {}'.format(total_balance, ref_coin))