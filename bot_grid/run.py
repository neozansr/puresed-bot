import ccxt
import time
import os
import sys

src_path = '../src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_config_system, get_config_params, get_exchange, get_currency, get_last_price, check_end_date
from func_update import append_error_log
from func_noti import print_pending_order, print_hold_assets, print_current_balance
from func_grid import cal_budget, clear_orders_grid, cancel_open_buy_orders_grid, open_buy_orders_grid, check_circuit_breaker, check_cut_loss, update_budget_grid


def run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    clear_orders_grid('buy', exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(config_system['idle_stage'])
    clear_orders_grid('sell', exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(config_system['idle_stage'])
    print_pending_order(quote_currency, open_orders_df_path)
    
    last_price = get_last_price(exchange, config_params)
    cont_flag = check_circuit_breaker(last_price, exchange, bot_name, base_currency, quote_currency, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path)

    if cont_flag == 1:
        cont_flag = check_cut_loss(last_price, exchange, bot_name, quote_currency, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, cash_flow_df_path)

        if cont_flag == 1:
            remain_budget, free_budget = cal_budget(config_params, open_orders_df_path)
            open_buy_orders_grid(remain_budget, free_budget, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)
            print_hold_assets(last_price, base_currency, quote_currency, config_params, open_orders_df_path)
            print_current_balance(last_price, exchange, quote_currency, config_params)

    end_date_flag, prev_date = check_end_date(bot_name, cash_flow_df_path)

    if end_date_flag == 1:
        update_budget_grid(last_price, prev_date, exchange, bot_name, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path)
        cancel_open_buy_orders_grid(exchange, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)


if __name__ == "__main__":
    config_system_path = 'config_system.json'
    config_params_path = 'config_params.json'
    last_loop_path = 'last_loop.json'
    transfer_path = 'transfer.json'
    open_orders_df_path = 'open_orders.csv'
    transactions_df_path = 'transactions.csv'
    error_log_df_path = 'error_log.csv'
    cash_flow_df_path = '../cash_flow/{}.csv'

    while True:
        config_system = get_config_system(config_system_path)
        config_params = get_config_params(config_params_path)
        
        if config_system['run_flag'] == 1:
            print('Start loop')
            try:
                run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                append_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
            time.sleep(config_system['idle_loop'])
        else:
            print('Stop process')
            time.sleep(config_system['idle_loop'])