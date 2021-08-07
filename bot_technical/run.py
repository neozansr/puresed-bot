import ccxt
import time
import os
import sys

src_path = '../src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_json, get_time, get_exchange, check_end_date
from func_update import append_error_log, update_timestamp
from func_technical import get_ohlcv, check_new_timestamp, get_current_position, manage_position, withdraw_position, check_drawdown, print_report_technical


def run_bot(config_system, config_params, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system, future=True)
    last_loop = get_json(last_loop_path)

    end_date_flag, prev_date = check_end_date(bot_name, cash_flow_df_path, transactions_df_path)
    
    if end_date_flag == 1:
        withdraw_position(prev_date, exchange, bot_name, config_system, config_params, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path)
        
    print(f'Time: {get_time()}')
    ohlcv_df = get_ohlcv(exchange, config_params)
    new_timestamp_flag = check_new_timestamp(ohlcv_df, config_params, last_loop)
    
    if new_timestamp_flag == True:
        manage_position(ohlcv_df, exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, profit_df_path)

    position = get_current_position(exchange, config_params)
    
    if position != None:
        if position['size'] != '0.0':
            check_drawdown(position, exchange, config_params, last_loop, bot_name, last_loop_path)
            print_report_technical(position, exchange, config_params)

    update_timestamp(last_loop_path)
    
    
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
        config_system = get_json(config_system_path)
        config_params = get_json(config_params_path)

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