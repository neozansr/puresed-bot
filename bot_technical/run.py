import ccxt
import time
import os
import sys

src_path = '../src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_config_system, get_config_params, get_time, get_exchange, get_currency_future, get_last_price, get_last_loop, check_end_date
from func_update import append_error_log
from func_noti import print_current_balance
from func_technical import get_ohlcv, update_timestamp, update_side, get_action, get_current_position, open_position, close_position, reduce_position, clear_orders_technical, append_profit_technical, update_budget_technical


def run_bot(config_system, config_params, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system, future=True)
    base_currency, quote_currency = get_currency_future(config_params)

    position = get_current_position(exchange, config_params)
    last_loop = get_last_loop(last_loop_path)

    end_date_flag, prev_date = check_end_date(bot_name, cash_flow_df_path)

    if end_date_flag == 1:
        last_price = get_last_price(exchange, config_params)
        withdraw_value = update_budget_technical(last_price, prev_date, position, exchange, bot_name, config_params, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path)
        
        if withdraw_value > 0:
            reverse_action = {'buy':'sell', 'sell':'buy'}
            last_price = get_last_price(exchange, config_params)
            action = reverse_action[position['side']]
            reduce_order = reduce_position(withdraw_value, action, exchange, config_params, open_orders_df_path)
            
            time.sleep(config_system['idle_stage'])
            clear_orders_technical(reduce_order, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path)
            append_profit_technical(reduce_order['size'], reduce_order, position, profit_df_path)

    ohlcv_df, last_timestamp = get_ohlcv(exchange, config_params)
    last_price = get_last_price(exchange, config_params)

    if last_loop['timestamp'] != last_timestamp:
        update_timestamp(last_timestamp, last_loop_path)
        action = get_action(ohlcv_df, config_params)

        if last_loop['side'] != action:
            if last_loop['side'] != 'start':
                if position != None:
                    close_order = close_position(action, position, exchange, config_params, open_orders_df_path)
                    
                    time.sleep(config_system['idle_stage'])
                    clear_orders_technical(close_order, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path)
                    append_profit_technical(close_order['size'], close_order, position, profit_df_path)

                open_order = open_position(action, exchange, config_params, open_orders_df_path)
                
                time.sleep(config_system['idle_stage'])
                clear_orders_technical(open_order, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path)
                append_profit_technical(open_order['size'], open_order, position, profit_df_path)

            update_side(action, last_loop_path)
    
    print_current_balance(last_price, exchange, quote_currency, config_params)

        
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
                run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
        else:
            print('Stop process')
        
        time.sleep(config_system['idle_loop'])