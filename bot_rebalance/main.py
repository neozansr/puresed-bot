import pandas as pd
import time
import os

from func_get import get_config_system, get_config_params, get_exchange, get_last_price, get_sequence, print_current_balance
from func_order import check_open_orders, rebalance_port


config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'


def run_bot(idle_stage, keys_path, config_params_path = config_params_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    symbol, fix_value, min_value = get_config_params(config_params_path)
    cont_flag = check_open_orders(exchange, bot_name, symbol, open_orders_df_path, transactions_df_path)
    time.sleep(idle_stage)
    last_price = get_last_price(exchange, symbol)
    
    if cont_flag == 1:
        rebalance_port(exchange, symbol, fix_value, min_value, last_price, open_orders_df_path)

    print_current_balance(exchange, symbol, last_price)


if __name__ == "__main__":
    loop_flag = True
    i = 0
    while loop_flag == True:
        print('start loop')
        loop_flag, idle_stage, min_idle, max_idle, keys_path = get_config_system(config_system_path)
        idle_loop_list = get_sequence(min_idle, max_idle)
        run_bot(idle_stage, keys_path)
        
        print('end loop')
        
        idle_loop = idle_loop_list[i]
        i += 1
        if i >= len(idle_loop_list):
            i = 0
            
        print('Wait {} seconds'.format(idle_loop))
        time.sleep(idle_loop)