import ccxt
import time
import os
import sys

home_path = '../'
src_path = home_path + 'src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_json, get_time, get_exchange, get_cash_value, check_end_date
from func_update import append_error_log, update_timestamp
from func_rebalance import update_sequence_loop, get_rebalance_flag, reset_order_loop, rebalance, clear_orders_rebalance, update_end_date_rebalance


def run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system)

    clear_orders_rebalance(exchange, bot_name, last_loop_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path)
    
    rebalance_flag = get_rebalance_flag(last_loop_path)

    if rebalance_flag == True:
        end_date_flag, prev_date = check_end_date(cash_flow_df_path, transactions_df_path)

        if end_date_flag == 1:
            update_end_date_rebalance(prev_date, exchange, config_params, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path)
        
        for symbol in config_params['symbol'].keys():
            print('Rebalance')
            print(symbol)
            rebalance(exchange, symbol, config_params, last_loop_path, open_orders_df_path)
        
        update_sequence_loop(config_params, last_loop_path)

        cash = get_cash_value(exchange)
        print(f"Cash: {cash} USD")
    
    update_timestamp(last_loop_path)


if __name__ == '__main__':
    config_system_path = 'config_system.json'
    config_params_path = 'config_params.json'
    last_loop_path = 'last_loop.json'
    transfer_path = 'transfer.json'
    open_orders_df_path = 'open_orders.csv'
    transactions_df_path = 'transactions.csv'
    queue_df_path = 'queue_{}.csv'
    profit_df_path = 'profit.csv'
    error_log_df_path = 'error_log.csv'
    cash_flow_df_path = 'cash_flow.csv'

    while True:
        config_system = get_json(config_system_path)
        config_params = get_json(config_params_path)
        idle_loop = config_system['idle_loop']

        if config_system['run_flag'] == 1:
            print("Start loop")
            try:
                run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.ExchangeError):
                append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print("End loop")

            last_loop = get_json(last_loop_path)
            timestamp = get_time()
            print(f"Time: {timestamp}")
            print(f"Next rebalance: {last_loop['next_rebalance_timestamp']}")
            print(f"Wait {idle_loop} seconds")
        else:
            print("Stop process")
            reset_order_loop(last_loop_path)
        
        time.sleep(idle_loop)