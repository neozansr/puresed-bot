import pandas as pd

from func_get import get_config_system, get_config_params, get_date, get_exchange, get_currency, get_last_price, get_balance, get_current_value, get_pending_order
from func_cal import cal_unrealised


def get_rebalance_text(text, sub_path, config_system_path, config_params_path, profit_df_path):
    config_system = get_config_system(sub_path + config_system_path)
    config_params = get_config_params(sub_path + config_params_path)

    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params, print_flag=False)

    cur_date = get_date()
    profit_df = pd.read_csv(sub_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]

    balance, cash = get_balance(last_price, exchange, config_params)
    current_value = get_current_value(last_price, exchange, base_currency)
    cash_flow = sum(today_profit_df['profit'])

    text += f'\nBalance: {balance:.2f} {quote_currency}'
    text += f'\nCurrent value: {current_value:.2f} {quote_currency}'
    text += f'\nCash: {cash:.2f} {quote_currency}'
    text += f'\nToday cash flow: {cash_flow:.2f} {quote_currency}'

    return text


def get_grid_text(text, sub_path, config_system_path, config_params_path, open_orders_df_path, transactions_df_path):
    config_system = get_config_system(sub_path + config_system_path)
    config_params = get_config_params(sub_path + config_params_path)

    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params, print_flag=False)
    
    cur_date = get_date()
    open_orders_df = pd.read_csv(sub_path + open_orders_df_path)
    transactions_df = pd.read_csv(sub_path + transactions_df_path)

    balance, _ = get_balance(last_price, exchange, config_params)
    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised(last_price, config_params, open_orders_df)

    today_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == cur_date]
    today_sell_df = today_transactions_df[today_transactions_df['side'] == 'sell']
    cash_flow = sum(today_sell_df['amount'] * config_params['grid'])
    
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(sub_path + open_orders_df_path)

    text += f'\nBalance: {balance:.2f} {quote_currency}'
    text += f'\nHold {amount:.4f} {base_currency} with {n_open_sell_oders} orders at {avg_price:.2f} {quote_currency}'
    text += f'\nUnrealised: {unrealised:.2f} {quote_currency}'
    text += f'\nToday cash flow: {cash_flow:.2f} {quote_currency}'
    text += f'\nMin buy price: {min_buy_price:.2f} {quote_currency}'
    text += f'\nMax buy price: {max_buy_price:.2f} {quote_currency}'
    text += f'\nMin sell price: {min_sell_price:.2f} {quote_currency}'
    text += f'\nMax sell price: {max_sell_price:.2f} {quote_currency}'

    return text