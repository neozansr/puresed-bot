import pandas as pd
import json
import ccxt


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    exchange = ccxt.binance(keys_dict)

    return exchange


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    budget = config_params['budget']
    grid = config_params['grid']
    value = config_params['value']
    symbol = config_params['symbol']

    return budget, grid, value, symbol


def get_transactions_df(transcations_df_path):
    transcations_df = pd.read_csv(transcations_df_path)

    return transcations_df


def get_orders_df(orders_df_path, symbol):
    orders_df = pd.read_csv(orders_df_path)
    orders_df = orders_df[orders_df['symbol'] == symbol]

    return orders_df


def get_open_oders(exchange, symbol):
    open_orders = exchange.fetch_open_orders(symbol)

    open_orders_list = []
    for order in open_orders:
        open_orders_list.append(order['id'])

    return open_orders_list


def get_latest_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    latest_price = ticker['last']

    return latest_price