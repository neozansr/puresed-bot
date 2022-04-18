import ccxt
import time
import os
import sys

home_path = '../'
src_path = home_path + 'src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_json, get_time, get_exchange, check_end_date
from func_update import append_error_log, update_timestamp
from func_technical import update_end_date_technical, check_close_position, check_open_position


def run_bot(config_system, config_params_path, last_loop_path, cash_flow_df_path, transactions_df_path, profit_df_path):
    exchange = get_exchange(config_system, future=True)

    end_date_flag, prev_date = check_end_date(cash_flow_df_path, transactions_df_path)
    
    if end_date_flag == 1:
        update_end_date_technical(prev_date, exchange, config_params_path, last_loop_path, transfer_path, cash_flow_df_path, profit_df_path)

    check_close_position()
    check_open_position()
        
    timestamp = get_time()
    print(f"Time: {timestamp}")
    update_timestamp(last_loop_path)
    
    
if __name__ == '__main__':
    config_system_path = 'config_system.json'
    config_params_path = 'config_params.json'
    last_loop_path = 'last_loop.json'
    transfer_path = 'transfer.json'
    transactions_df_path = 'transactions.csv'
    profit_df_path = 'profit.csv'
    error_log_df_path = 'error_log.csv'
    cash_flow_df_path = home_path + 'cash_flow/{}.csv'

    while True:
        config_system = get_json(config_system_path)

        if config_system['run_flag'] == 1:
            print("Start loop")
            try:
                run_bot(config_system, config_params_path, last_loop_path, cash_flow_df_path, transactions_df_path, profit_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.ExchangeError):
                append_error_log('ConnectionError', error_log_df_path)
                print("No connection: Skip the loop")
        
            print("End loop")
        else:
            print("Stop process")
        
        time.sleep(config_system['idle_loop'])