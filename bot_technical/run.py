import ccxt
import time
import sys

sys.path.insert(1, '../src')
import func_get
import func_update
import func_signal
import func_technical


def run_bot(config_system, config_params_path, last_loop_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    exchange = func_get.get_exchange(config_system, future=True)
    config_params = func_get.get_json(config_params_path)

    end_date_flag, prev_date = func_get.check_end_date(cash_flow_df_path, transactions_df_path)
    
    if end_date_flag:
        # update end date
        pass

    ohlcv_df_dict = func_technical.get_ohlcv_df_dict(exchange, config_params)
    ohlcv_df_dict = func_signal.add_signal(ohlcv_df_dict, config_params)

    # check stop

    # check time

    position_list = func_technical.get_position_list(exchange)

    for symbol in position_list:
        # close position
        pass

    open_symbol_list = func_technical.get_open_symbol_list(exchange, config_params)
    
    for symbol in open_symbol_list:
        func_technical.open_position(exchange, symbol, config_system, config_params, ohlcv_df_dict, last_loop_path, transactions_df_path)
    
    func_update.update_timestamp(last_loop_path)
    timestamp = func_get.get_time()
    print(f"Time: {timestamp}")
    
    
if __name__ == '__main__':
    config_system_path = 'config_system.json'
    config_params_path = 'config_params.json'
    last_loop_path = 'last_loop.json'
    transfer_path = 'transfer.json'
    open_orders_df_path = 'open_orders.csv'
    transactions_df_path = 'transactions.csv'
    profit_df_path = 'profit.csv'
    error_log_df_path = 'error_log.csv'
    cash_flow_df_path = 'cash_flow.csv'

    while True:
        config_system = func_get.get_json(config_system_path)
        idle_loop = config_system['idle_loop']

        if config_system['run_flag'] == 1:
            print("Start loop")
            
            try:
                run_bot(config_system, config_params_path, last_loop_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.ExchangeError):
                func_update.append_error_log('ConnectionError', error_log_df_path)
                print("No connection: Skip the loop")
        
            print("End loop")
            print(f"Wait {idle_loop} seconds")
        else:
            print("Stop process")
        
        time.sleep(idle_loop)