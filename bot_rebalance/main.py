import ccxt
import time
import os

from func_get import get_config_system, get_config_params, get_time, get_exchange, get_currency, get_last_price, get_current_value, get_idle_loop, update_budget, get_withdraw_flag, update_withdraw_flag, reset_n_loop
from func_order import check_open_orders, rebalance, append_error_log
from func_noti import print_current_balance


config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
transfer_path = 'transfer.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
queue_df_path = 'queue.csv'
profit_df_path = 'profit.csv'
error_log_df_path = 'error_log.csv'
cash_flow_df_path = '../cash_flow/{}.csv'


def run_bot(idle_stage, keys_path, config_params_path = config_params_path, last_loop_path = last_loop_path, transfer_path = transfer_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path, error_log_df_path = error_log_df_path, cash_flow_df_path = cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    symbol, fix_value, min_value = get_config_params(config_params_path)
    base_currency, quote_currency = get_currency(symbol)
    
    # get withdraw_flag from last_loop to prevent pending queue by ConnectionError during last loop
    withdraw_flag = get_withdraw_flag(last_loop_path)

    if withdraw_flag == 0:
        # skip loop if the order from last loop hasn't been recieved by the server
        cont_flag = check_open_orders(exchange, bot_name, symbol, base_currency, quote_currency, 'lifo', open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path)

        if cont_flag == 1:
            withdraw_flag = update_budget(exchange, bot_name, symbol, fix_value, config_params_path, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path)
        
            if withdraw_flag == 1:
                order_type = 'market'
            else:
                order_type = 'limit'

            time.sleep(idle_stage)
            last_price = get_last_price(exchange, symbol)
            current_value = get_current_value(exchange, symbol, last_price)
            print_current_balance(exchange, current_value, symbol, quote_currency, last_price)
            rebalance(exchange, symbol, base_currency, quote_currency, fix_value, current_value, min_value, order_type, open_orders_df_path, error_log_df_path)
            
            if withdraw_flag == 1:
                update_withdraw_flag(last_loop_path, True)

    if withdraw_flag == 1:
        # if params change, queues have to be cleared by FIFO within the loop
        time.sleep(idle_stage)
        _ = check_open_orders(exchange, bot_name, symbol, base_currency, quote_currency, 'fifo', open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path)
        update_withdraw_flag(last_loop_path, False)


if __name__ == "__main__":
    while True:
        run_flag, idle_stage, keys_path = get_config_system(config_system_path)
        idle_loop = get_idle_loop(last_loop_path)

        if run_flag == 1:
            print('Start loop')
            try:
                run_bot(idle_stage, keys_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
            print(get_time())
            print('Wait {} seconds'.format(idle_loop))
        else:
            print('Stop process')
            reset_n_loop(last_loop_path)
        
        time.sleep(idle_loop)