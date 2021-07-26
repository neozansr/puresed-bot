import ccxt
import time
import os

from func_get import get_config_system, get_config_params, get_exchange, get_currency, get_last_price, update_last_loop_price, update_budget
from func_cal import cal_budget
from func_order import check_orders_status, cancel_open_buy_orders, open_buy_orders, append_error_log, check_circuit_breaker, check_cut_loss
from func_noti import print_pending_order, print_hold_assets, print_current_balance_grid


def run_bot(config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    check_orders_status('buy', exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(config_system['idle_stage'])
    check_orders_status('sell', exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(config_system['idle_stage'])
    print_pending_order(quote_currency, open_orders_df_path)
    
    last_price = get_last_price(exchange, config_params)
    cont_flag = check_circuit_breaker(exchange, bot_name, base_currency, quote_currency, last_price, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path)

    if cont_flag == 1:
        cont_flag = check_cut_loss(exchange, bot_name, quote_currency, last_price, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, cash_flow_df_path)

        if cont_flag == 1:
            remain_budget, free_budget = cal_budget(config_params, open_orders_df_path)
            open_buy_orders(remain_budget, free_budget, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)
            print_hold_assets(last_price, base_currency, quote_currency, config_params, open_orders_df_path)
            print_current_balance_grid(exchange, last_price, quote_currency, config_params)

    change_params_flag = update_budget(exchange, bot_name, last_price, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path)

    if change_params_flag == 1:
        cancel_open_buy_orders(exchange, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)


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