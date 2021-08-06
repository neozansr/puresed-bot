import ccxt
import time
import os
import sys

src_path = '../src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_config_system, get_config_params, get_time, get_exchange, get_last_loop, check_end_date
from func_update import append_error_log
from func_technical import get_ohlcv, get_current_position, manage_position, withdraw_position, check_liquidate, print_report_technical


def run_bot(config_system, config_params, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system, future=True)
    last_loop = get_last_loop(last_loop_path)

    end_date_flag, prev_date = check_end_date(bot_name, cash_flow_df_path, transactions_df_path)
    
    if end_date_flag == 1:
        withdraw_position(prev_date, exchange, bot_name, config_system, config_params, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path)
        
    ohlcv_df, last_timestamp = get_ohlcv(exchange, config_params)
    
    print(f'Time: {get_time()}')
    print(f'Last timestamp: {last_timestamp}')

    if last_loop['timestamp'] != last_timestamp:
        manage_position(ohlcv_df, last_timestamp, exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, profit_df_path)

    position = get_current_position(exchange, config_params)
    check_liquidate(position, last_loop, bot_name, last_loop_path)
    print_report_technical(position, exchange, config_params)
    
    
if __name__ == "__main__":
    config_system_path = 'config_system.json'
    config_params_path = 'config_params.json'
    last_loop_path = 'last_loop.json'
    transfer_path = 'transfer.json'
    open_orders_df_path = 'open_orders.csv'
    transactions_df_path = 'transactions.csv'
    profit_df_path = 'profit.csv'
    error_log_df_path = 'error_log.csv'
    cash_flow_df_path = '../cash_flow/{}.csv'

    while True:
        config_system = get_config_system(config_system_path)
        config_params = get_config_params(config_params_path)

        if config_system['run_flag'] == 1:
            print('Start loop')
            try:
                run_bot(config_system, config_params, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
        else:
            print('Stop process')
        
        time.sleep(config_system['idle_loop'])