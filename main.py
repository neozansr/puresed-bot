import pandas as pd
import datetime as dt

from func_get import *
from func_cal import *
from func_order import *

keys_path = '../_keys/binance_keys.json'
config_params_path = 'config_params.json'
transcations_df_path = 'transactions.csv'
orders_df_path = 'orders.csv'

    
def run_bot(keys_path = keys_path, config_params_path = config_params_path, transcations_df_path = transcations_df_path, orders_df_path = orders_df_path):
    exchange = get_exchange(keys_path)
    budget, grid, value, symbol = get_config_params(config_params_path)
    transcations_df = get_transactions_df(transcations_df_path)
    orders_df = get_orders_df(orders_df_path, symbol)
    open_orders_list = get_open_oders(exchange, symbol)

    orders_df = remove_close_orders(open_orders_list, orders_df, symbol)

    latest_price = get_latest_price(exchange, symbol)
    buying_power = cal_buying_power(budget, orders_df)
    buy_orders_price_list, n_order = cal_buy_orders(latest_price, grid, value, buying_power)
    cancel_orders_price_list, new_orders_price_list = organize_buy_orders(orders_df, buy_orders_price_list, n_order, latest_price, grid)

    orders_df = cancel_invalid_buy_orders(exchange, orders_df, cancel_orders_price_list)
    orders_df, transcations_df = open_buys_order(exchange, orders_df, transcations_df, new_orders_price_list, symbol, grid, value)

    return None


if __name__ == "__main__":
    run_bot()