import ccxt
import pandas as pd
import time
import os

from func_get import get_config_system, get_config_params, get_exchange, get_last_price, update_last_loop_price
from func_cal import cal_n_order
from func_order import check_orders_status, open_buy_orders, update_error_log, check_circuit_breaker, reinvest
from func_noti import print_pending_order, print_hold_assets, print_current_balance

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
assets_df_path = 'assets.csv'
error_log_df_path = 'error_log.csv'
cash_flow_df_path = '../cash_flow/{}.csv'

    
def run_bot(idle_stage, idle_loop, idle_rest, keys_path, config_params_path = config_params_path, last_loop_path = last_loop_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path, assets_df_path = assets_df_path, error_log_df_path = error_log_df_path, cash_flow_df_path = cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    symbol, init_budget, budget, grid, value, reinvest_ratio, start_safety, circuit_limit, decimal = get_config_params(config_params_path)
    last_price = get_last_price(exchange, symbol)
    check_orders_status(exchange, bot_name, 'buy', symbol, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(idle_stage)
    check_orders_status(exchange, bot_name, 'sell', symbol, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(idle_stage)
    print_pending_order(symbol, open_orders_df_path)
    n_order, n_sell_order, n_open_order = cal_n_order(budget, value, open_orders_df_path)
    cont_flag = check_circuit_breaker(bot_name, exchange, symbol, last_price, circuit_limit, idle_stage, idle_rest, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path)

    if cont_flag == 1:
        update_last_loop_price(exchange, symbol, last_loop_path)
        open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, start_safety, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)
        print_hold_assets(symbol, grid, last_price, open_orders_df_path)
        print_current_balance(exchange, symbol, last_price)

    reinvest(exchange, bot_name, reinvest_ratio, init_budget, budget, symbol, grid, value, n_order, last_price, config_params_path, open_orders_df_path, transactions_df_path, cash_flow_df_path)


if __name__ == "__main__":
    while True:
        run_flag, idle_stage, idle_loop, idle_rest, keys_path = get_config_system(config_system_path)
        
        if run_flag == 1:
            print('Start loop')
            try:
                run_bot(idle_stage, idle_loop, idle_rest, keys_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                update_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
            time.sleep(idle_loop)
        else:
            print('Stop process')
            time.sleep(idle_loop)