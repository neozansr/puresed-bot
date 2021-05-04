import pandas as pd
import datetime as dt

from func_get import *
from func_cal import *
from func_order import *

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
keys_path = '_keys/binance_keys.json'
open_orders_df_path = 'open_orders.csv'
transcations_df_path = 'transactions.csv'

    
def run_bot(idle_stage, keys_path = keys_path, config_params_path = config_params_path, transcations_df_path = transcations_df_path, open_orders_df_path = open_orders_df_path):
    exchange = get_exchange(keys_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transcations_df = pd.read_csv(transcations_df_path)
    symbol, budget, grid, value, min_price, max_price = get_config_params(config_params_path)
    latest_price = get_latest_price(exchange, symbol)
    open_orders_df, transcations_df = check_orders_statue(exchange, 'buy', symbol, grid, value, latest_price, open_orders_df, transcations_df)
    time.sleep(idle_stage)
    open_orders_df, transcations_df = check_orders_statue(exchange, 'sell', symbol, grid, value, latest_price, open_orders_df, transcations_df)
    time.sleep(idle_stage)
    n_order, n_sell_order = cal_n_order(open_orders_df, budget, value)
    open_orders_df, transcations_df = open_buy_orders(exchange, n_order, n_sell_order, symbol, grid, value, latest_price, min_price, max_price, open_orders_df, transcations_df)

    return open_orders_df, transcations_df

if __name__ == "__main__":
    run_flag = True
    while run_flag == True:
        print('bot run')
        run_flag, idle_stage, idle_loop = get_config_system(config_system_path)
        open_orders_df, transcations_df = run_bot(idle_stage)
        open_orders_df.to_csv(open_orders_df_path, index = False)
        transcations_df.to_csv(transcations_df_path, index = False)
        time.sleep(idle_loop)