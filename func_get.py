import pandas as pd
import datetime as dt
from dateutil import tz
import json
import ccxt


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

    return symbol, budget, grid, value, min_price, max_price


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.binance({'apiKey': keys_dict['apiKey'],
                             'secret': keys_dict['secret'],
                             'enableRateLimit': True,
                             'options': {'newOrderRespType': 'FULL'}})

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