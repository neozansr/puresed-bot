import ccxt
import pandas as pd
import time
import os

from func_get import get_config_system, get_config_params, get_exchange, get_last_price, get_current_value, get_idle_loop, reset_n_loop
from func_order import check_open_orders, rebalance, update_error_log, update_cash_flow
from func_noti import print_current_balance


config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
error_log_df_path = 'error_log.csv'
cash_flow_df_path = '../cash_flow/{}.csv'


def run_bot(idle_stage, keys_path, config_params_path = config_params_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path, error_log_df_path = error_log_df_path, cash_flow_df_path = cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    symbol, fix_value, min_value = get_config_params(config_params_path)
    cont_flag = check_open_orders(exchange, bot_name, symbol, open_orders_df_path, transactions_df_path, error_log_df_path)

    if cont_flag == 1:
        time.sleep(idle_stage)
        last_price = get_last_price(exchange, symbol)
        current_value = get_current_value(exchange, symbol, last_price)
        rebalance(exchange, current_value, symbol, fix_value, min_value, last_price, open_orders_df_path, error_log_df_path)

    print_current_balance(exchange, current_value, symbol, last_price)
    update_cash_flow(exchange, bot_name, symbol, fix_value, current_value, last_price, cash_flow_df_path)


if __name__ == "__main__":
    while True:
        run_flag, idle_stage, keys_path = get_config_system(config_system_path)
        idle_loop = get_idle_loop(last_loop_path)

        if run_flag == 1:
            print('Start loop')
            try:
                run_bot(idle_stage, keys_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                update_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')    
            print('Wait {} seconds'.format(idle_loop))
        else:
            print('Stop process')
            reset_n_loop(last_loop_path)
        
        time.sleep(idle_loop)