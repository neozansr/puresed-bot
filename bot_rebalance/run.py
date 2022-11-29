import ccxt
import time
import os
import sys

sys.path.insert(1, '../src')
import func_get
import func_update
import func_rebalance


def run_bot(config_system, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = func_get.get_exchange(config_system)
    config_params = func_get.get_json(config_params_path)
    
    end_date_flag, prev_date = func_get.check_end_date(cash_flow_df_path, transactions_df_path)

    if end_date_flag:
        func_rebalance.update_end_date_rebalance(prev_date, exchange, config_system, config_params, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path)

    rebalance_flag = func_rebalance.get_rebalance_flag(exchange, config_params, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path)

    if rebalance_flag:
        func_rebalance.clear_orders_rebalance(exchange, bot_name, config_system, config_params, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, cash_flow_df_path, resend_flag=False)
        
        for symbol in config_params['symbol']:
            func_rebalance.rebalance(exchange, symbol, config_params, last_loop_path, transfer_path, open_orders_df_path, profit_df_path, cash_flow_df_path)
        
        func_rebalance.update_sequence_loop(config_params, last_loop_path)

        cash = func_get.get_quote_currency_value(exchange, symbol)
        print(f"Cash: {cash} USD")
    else:
        func_rebalance.clear_orders_rebalance(exchange, bot_name, config_system, config_params, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, cash_flow_df_path, resend_flag=True)

    func_update.update_timestamp(last_loop_path)

    timestamp = func_get.get_time()
    print(f"Time: {timestamp}")

    last_loop = func_get.get_json(last_loop_path)
    print(f"Next rebalance: {last_loop['next_rebalance_timestamp']}")


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
        config_system = func_get.get_json(config_system_path)
        idle_loop = config_system['idle_loop']

        if config_system['run_flag'] == 1:
            print("Start loop")
            
            try:
                run_bot(config_system, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError, ccxt.ExchangeError):
                func_update.append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
            
            print("End loop")
            print(f"Wait {idle_loop} seconds")
        else:
            print("Stop process")
            func_rebalance.reset_order_loop(last_loop_path)
        
        time.sleep(idle_loop)