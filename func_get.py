import pandas as pd
import datetime as dt
from dateutil import tz
import json
import ccxt

from noti import *


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    run_flag = config_system['run_flag']
    idle_stage = config_system['idle_stage']
    idle_loop = config_system['idle_loop']

    return run_flag, idle_stage, idle_loop


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

    return latest_price


def convert_tz(utc):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = utc.replace(tzinfo=from_zone).astimezone(to_zone)
    
    return utc


def get_time(datetime_raw):
    datetime_str, _, us = datetime_raw.partition('.')
    datetime_utc = dt.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
    us = int(us.rstrip('Z'), 10)

    datetime_th = convert_tz(datetime_utc)
    
    return datetime_th + dt.timedelta(microseconds = us)


def get_assets(open_orders_df, symbol, latest_price):
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    price_list = [x - 10 for x in open_sell_orders_df['price']]
    amount_list = open_sell_orders_df['amount'].to_list()

    amount = sum(amount_list)
    total_value = sum([i * j for i, j in zip(price_list, amount_list)])
    
    try:
        avg_price = total_value / amount
    except ZeroDivisionError:
        avg_price = 0

    unrealised_loss = (latest_price - avg_price) * amount

    assets_dict = {'datetime': dt.datetime.now(),
                   'latest_price': latest_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised_loss': unrealised_loss}

    assets_df = pd.DataFrame(assets_dict, index = [0])
    assets_df.to_csv('assets.csv', index = False)
    message = 'hold {} {} with {} orders at {} USDT: {} USDT unrealised_loss'.format(amount, symbol.split('/')[0], len(open_sell_orders_df), avg_price, unrealised_loss)

    print(message)