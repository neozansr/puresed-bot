import ccxt
import time
import os
import sys

src_path = '../src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_config_system, get_config_params, get_time, get_exchange, get_currency, get_last_price, get_current_value, check_end_date
from func_update import append_error_log
from func_noti import print_current_balance, print_current_value
from func_rebalance import get_series_loop, reset_order_loop, clear_orders_rebalance, rebalance, update_withdraw_flag, update_budget_rebalance


def run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    
    end_date_flag, prev_date = check_end_date(bot_name, cash_flow_df_path)

    if end_date_flag == 1:
        last_price = get_last_price(exchange, config_params)
        withdraw_flag = update_budget_rebalance(last_price, prev_date, exchange, bot_name, config_params, config_params_path, last_loop_path, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path)
        update_withdraw_flag(last_loop_path, withdraw_flag)
        time.sleep(config_system['idle_stage'])        
    else:
        withdraw_flag = 0

    if withdraw_flag == 1:
        # force sell from withdrawal
        method = 'fifo'
    else:
        method = 'lifo'

    last_price = get_last_price(exchange, config_params)
    current_value = get_current_value(last_price, exchange, base_currency)
    
    print_current_balance(last_price, exchange, quote_currency, config_params)
    print_current_value(last_price, current_value, exchange, quote_currency, config_params)
    
    rebalance(current_value, exchange, base_currency, quote_currency, config_params, open_orders_df_path, error_log_df_path)
    
    time.sleep(config_system['idle_stage'])
    clear_orders_rebalance(method, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path)
    update_withdraw_flag(last_loop_path, False)


if __name__ == "__main__":
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

    while True:
        config_system = get_config_system(config_system_path)
        config_params = get_config_params(config_params_path)
        idle_loop = get_series_loop(last_loop_path)

        if config_system['run_flag'] == 1:
            print('Start loop')
            try:
                run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
            print(get_time())
            print(f'Wait {idle_loop} seconds')
        else:
            print('Stop process')
            reset_order_loop(last_loop_path)
        
        time.sleep(idle_loop)