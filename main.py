import pandas as pd
import datetime as dt

from func_get import *
from func_cal import *
from func_order import *

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
keys_path = '_keys/kucoin_keys.json'
open_orders_df_path = 'open_orders.csv'
assets_df_path = 'assets.csv'
transactions_df_path = 'transactions.csv'

    
def run_bot(idle_stage, keys_path = keys_path, config_params_path = config_params_path, transactions_df_path = transactions_df_path, assets_df_path = assets_df_path, open_orders_df_path = open_orders_df_path):
    exchange = get_exchange(keys_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transactions_df = pd.read_csv(transactions_df_path)
    symbol, budget, grid, value, min_price, max_price, fee_percent, start_market = get_config_params(config_params_path)
    latest_price = get_latest_price(exchange, symbol)
    open_orders_df, transactions_df = check_orders_status(exchange, 'buy', symbol, grid, latest_price, fee_percent, open_orders_df, transactions_df)
    time.sleep(idle_stage)
    open_orders_df, transactions_df = check_orders_status(exchange, 'sell', symbol, grid, latest_price, fee_percent, open_orders_df, transactions_df)
    time.sleep(idle_stage)
    latest_price = get_latest_price(exchange, symbol)
    n_order, n_sell_order, n_open_order = cal_n_order(open_orders_df, budget, value)
    open_orders_df, transactions_df = open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, latest_price, fee_percent, min_price, max_price, start_market, open_orders_df, transactions_df)
    get_assets(open_orders_df, symbol, latest_price)

    return open_orders_df, transactions_df

if __name__ == "__main__":
    run_flag = True
    while run_flag == True:
        print('start loop')
        run_flag, idle_stage, idle_loop = get_config_system(config_system_path)
        open_orders_df, transactions_df = run_bot(idle_stage)
        open_orders_df.to_csv(open_orders_df_path, index = False)
        transactions_df.to_csv(transactions_df_path, index = False)
        print('end loop')
        time.sleep(idle_loop)