import ccxt
import time
import os

from func_get import get_config_system, get_config_params, get_time, get_exchange, get_currency, get_last_price, get_current_value, get_last_loop, get_idle_loop, update_budget, update_withdraw_flag, reset_n_loop
from func_order import check_open_orders, rebalance, append_error_log
from func_noti import print_current_balance_rebalance


def run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    
    # get withdraw_flag from last_loop to prevent pending queue by ConnectionError during last loop
    last_loop = get_last_loop(last_loop_path)

    if last_loop['withdraw_flag'] == 0:
        # skip loop if the order from last loop hasn't been recieved by the server
        cont_flag = check_open_orders('lifo', exchange, bot_name, base_currency, quote_currency, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path)

        if cont_flag == 1:
            withdraw_flag = update_budget(exchange, bot_name, base_currency, config_params, config_params_path, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path)
        
            if withdraw_flag == 1:
                order_type = 'market'
            else:
                order_type = 'limit'

            time.sleep(config_system['idle_stage'])
            last_price = get_last_price(exchange, config_params)
            current_value = get_current_value(last_price, exchange, base_currency)
            print_current_balance_rebalance(current_value, last_price, exchange, quote_currency, config_params)
            rebalance(order_type, current_value, exchange, base_currency, quote_currency, config_params, open_orders_df_path, error_log_df_path)
            
            if withdraw_flag == 1:
                update_withdraw_flag(last_loop_path, True)

    if withdraw_flag == 1:
        # if params change, queues have to be cleared by FIFO within the loop
        time.sleep(config_system['idle_stage'])
        _ = check_open_orders('fifo', exchange, bot_name, base_currency, quote_currency, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path)
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
        idle_loop = get_idle_loop(last_loop_path)

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
            reset_n_loop(last_loop_path)
        
        time.sleep(idle_loop)