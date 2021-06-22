import ccxt
import pandas as pd
import time
import os

from func_get import get_config_system, get_config_params, get_exchange, get_currency, get_last_price, update_last_loop_price
from func_cal import cal_budget
from func_order import check_orders_status, cancel_open_buy_orders, open_buy_orders, update_error_log, check_circuit_breaker, check_cut_loss, reinvest
from func_noti import print_pending_order, print_hold_assets, print_current_balance

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
transfer_path = 'transfer.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
assets_df_path = 'assets.csv'
error_log_df_path = 'error_log.csv'
cash_flow_df_path = '../cash_flow/{}.csv'

    
def run_bot(idle_stage, idle_loop, idle_rest, keys_path, config_params_path = config_params_path, last_loop_path = last_loop_path, transfer_path = transfer_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path, assets_df_path = assets_df_path, error_log_df_path = error_log_df_path, cash_flow_df_path = cash_flow_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    symbol, init_budget, budget, grid, value, fluctuation_rate, reinvest_ratio, start_safety, circuit_limit, decimal = get_config_params(config_params_path)
    base_currency, quote_currency = get_currency(symbol)
    last_price = get_last_price(exchange, symbol)
    check_orders_status(exchange, bot_name, 'buy', symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(idle_stage)
    check_orders_status(exchange, bot_name, 'sell', symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)
    time.sleep(idle_stage)
    print_pending_order(symbol, quote_currency, open_orders_df_path)
    cont_flag = check_circuit_breaker(bot_name, exchange, symbol, base_currency, quote_currency, last_price, grid, value, circuit_limit, idle_stage, idle_rest, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path)

    if cont_flag == 1:
        check_cut_loss(exchange, bot_name, symbol, quote_currency, last_price, grid, value, config_params_path, last_loop_path, open_orders_df_path, cash_flow_df_path, idle_stage)
        update_last_loop_price(exchange, symbol, last_loop_path)
        remain_budget, free_budget = cal_budget(budget, grid, open_orders_df_path)
        open_buy_orders(exchange, bot_name, remain_budget, free_budget, symbol, base_currency, quote_currency, grid, value, start_safety, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path)
        print_hold_assets(symbol, base_currency, quote_currency, last_price, grid, open_orders_df_path)
        print_current_balance(exchange, symbol, quote_currency, last_price)

    reinvest_flag = reinvest(exchange, bot_name, symbol, last_price, init_budget, budget, grid, value, fluctuation_rate, reinvest_ratio, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path)

    if reinvest_flag == 1:
        cancel_open_buy_orders(exchange, symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)


if __name__ == "__main__":
    while True:
        run_flag, idle_stage, idle_loop, idle_rest, keys_path = get_config_system(config_system_path)
        
        if run_flag == 1:
            print('Start loop')
            try:
                run_bot(idle_stage, idle_loop, idle_rest, keys_path)
            except (ccxt.RequestTimeout, ccxt.NetworkError):
                update_error_log('ConnectionError', error_log_df_path)
                print('No connection: Skip the loop')
        
            print('End loop')
            time.sleep(idle_loop)
        else:
            print('Stop process')
            time.sleep(idle_loop)