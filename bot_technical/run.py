import ccxt
import time
import sys

sys.path.insert(1, '../src')
import func_get
import func_update
import func_technical

home_path = '../'


def run_bot(config_system, config_params_path, last_loop_path, cash_flow_df_path, transactions_df_path, profit_df_path):
    exchange = func_get.get_exchange(config_system, future=True)

    end_date_flag, prev_date = func_get.check_end_date(cash_flow_df_path, transactions_df_path)
    
    if end_date_flag == 1:
        func_technical.update_end_date_technical(prev_date, exchange, config_params_path, last_loop_path, transfer_path, cash_flow_df_path, profit_df_path)

    func_technical.check_close_position()
    func_technical.check_open_position()
        
    timestamp = func_get.get_time()
    print(f"Time: {timestamp}")
    func_update.update_timestamp(last_loop_path)
    
    
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
        config_system = func_get.get_json(config_system_path)

        if config_system['run_flag'] == 1:
            print("Start loop")
            try:
                run_bot(config_system, config_params_path, last_loop_path, cash_flow_df_path, transactions_df_path, profit_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.ExchangeError):
                func_update.append_error_log('ConnectionError', error_log_df_path)
                print("No connection: Skip the loop")
        
            print("End loop")
        else:
            print("Stop process")
        
        time.sleep(config_system['idle_loop'])