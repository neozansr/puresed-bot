import pandas as pd
import time
import os

from func_get import get_config_system, get_config_params, get_exchange, get_last_price, print_pending_order, print_hold_assets, print_current_balance
from func_cal import cal_n_order
from func_order import check_orders_status, open_buy_orders

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
assets_df_path = 'assets.csv'

    
def run_bot(idle_stage, keys_path, config_params_path = config_params_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path, assets_df_path = assets_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    symbol, budget, grid, value, min_price, max_price, fee_percent, start_safety = get_config_params(config_params_path)
    last_price = get_last_price(exchange, symbol)
    check_orders_status(exchange, bot_name, 'buy', symbol, grid, fee_percent, open_orders_df_path, transactions_df_path)
    time.sleep(idle_stage)
    check_orders_status(exchange, bot_name, 'sell', symbol, grid, fee_percent, open_orders_df_path, transactions_df_path)
    time.sleep(idle_stage)
    print_pending_order(symbol, open_orders_df_path)
    n_order, n_sell_order, n_open_order = cal_n_order(budget, value, open_orders_df_path)
    open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, fee_percent, min_price, max_price, start_safety, open_orders_df_path, transactions_df_path)
    print_hold_assets(symbol, grid, last_price, open_orders_df_path)
    print_current_balance(exchange, symbol, last_price)


if __name__ == "__main__":
    loop_flag = True
    while loop_flag == True:
        print('start loop')
        loop_flag, idle_stage, idle_loop, keys_path = get_config_system(config_system_path)
        run_bot(idle_stage, keys_path)
        
        print('end loop')
        time.sleep(idle_loop)