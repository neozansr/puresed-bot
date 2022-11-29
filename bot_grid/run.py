import ccxt
import time
import os
import sys

sys.path.insert(1, '../src')
import func_get
import func_update
import func_grid


def run_bot(config_system, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = func_get.get_exchange(config_system)
    config_params = func_get.get_json(config_params_path)
    
    func_grid.clear_orders_grid('buy', exchange, bot_name, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    func_grid.clear_orders_grid('sell', exchange, bot_name, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    func_grid.print_report_grid(exchange, config_params, open_orders_df_path)
    
    cont_flag = func_grid.check_circuit_breaker(exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path)

    if cont_flag:
        func_grid.open_buy_orders_grid(exchange, config_params, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)

    end_date_flag, prev_date = func_get.check_end_date(cash_flow_df_path, transactions_df_path)

    if end_date_flag:
        func_grid.update_end_date_grid(prev_date, exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)

    func_update.update_timestamp(last_loop_path)


if __name__ == '__main__':
    config_system_path = 'config_system.json'
    config_params_path = 'config_params.json'
    last_loop_path = 'last_loop.json'
    transfer_path = 'transfer.json'
    open_orders_df_path = 'open_orders.csv'
    transactions_df_path = 'transactions.csv'
    error_log_df_path = 'error_log.csv'
    cash_flow_df_path = 'cash_flow.csv'

    while True:
        config_system = func_get.get_json(config_system_path)
        idle_loop = config_system['idle_loop']
        
        if config_system['run_flag'] == 1:
            print("Start loop")
            try:
                run_bot(config_system, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.ExchangeError):
                func_update.append_error_log('ConnectionError', error_log_df_path)
                print("No connection: Skip the loop")
        
            print("End loop")
            print(f"Wait {idle_loop} seconds")
        else:
            print("Stop process")
        
        time.sleep(idle_loop)