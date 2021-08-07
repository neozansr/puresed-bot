import pandas as pd

from func_get import get_json, get_date, get_exchange, get_currency, get_currency_future, get_last_price, get_base_currency_value, get_quote_currency_value, get_pending_order
from func_cal import cal_unrealised
from func_technical import get_current_position


def get_rebalance_text(text, sub_path, config_system_path, config_params_path, last_loop_path, profit_df_path):
    config_system = get_json(sub_path + config_system_path)
    config_params = get_json(sub_path + config_params_path)

    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)

    cur_date = get_date()
    profit_df = pd.read_csv(sub_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)
    balance_value = current_value + cash
    cash_flow = sum(today_profit_df['profit'])

    last_loop = get_json(sub_path + last_loop_path)
    last_timestamp = last_loop['timestamp']

    text += f'\nBalance: {balance_value:.2f} {quote_currency}'
    text += f'\nCurrent value: {current_value:.2f} {quote_currency}'
    text += f'\nCash: {cash:.2f} {quote_currency}'
    text += f'\nToday cash flow: {cash_flow:.2f} {quote_currency}'

    text += f'\n\nLast active: {last_timestamp}'

    return text


def get_grid_text(text, sub_path, config_system_path, config_params_path, last_loop_path, open_orders_df_path, transactions_df_path):
    config_system = get_json(sub_path + config_system_path)
    config_params = get_json(sub_path + config_params_path)

    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)
    
    cur_date = get_date()
    open_orders_df = pd.read_csv(sub_path + open_orders_df_path)
    transactions_df = pd.read_csv(sub_path + transactions_df_path)

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)
    balance_value = current_value + cash

    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised(last_price, config_params, open_orders_df)

    today_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == cur_date]
    today_sell_df = today_transactions_df[today_transactions_df['side'] == 'sell']
    cash_flow = sum(today_sell_df['amount'] * config_params['grid'])
    
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(sub_path + open_orders_df_path)

    last_loop = get_json(sub_path + last_loop_path)
    last_timestamp = last_loop['timestamp']

    text += f'\nBalance: {balance_value:.2f} {quote_currency}'
    text += f'\nHold {amount:.4f} {base_currency} with {n_open_sell_oders} orders at {avg_price:.2f} {quote_currency}'
    text += f'\nUnrealised: {unrealised:.2f} {quote_currency}'
    text += f'\nToday cash flow: {cash_flow:.2f} {quote_currency}'
    text += f'\nMin buy price: {min_buy_price:.2f} {quote_currency}'
    text += f'\nMax buy price: {max_buy_price:.2f} {quote_currency}'
    text += f'\nMin sell price: {min_sell_price:.2f} {quote_currency}'
    text += f'\nMax sell price: {max_sell_price:.2f} {quote_currency}'

    text += f'\n\nLast active: {last_timestamp}'

    return text


def get_technical_text(text, sub_path, config_system_path, config_params_path, last_loop_path):
    config_system = get_json(sub_path + config_system_path)
    config_params = get_json(sub_path + config_params_path)

    exchange = get_exchange(config_system, future=True)
    base_currency, quote_currency = get_currency_future(config_params)
    last_price = get_last_price(exchange, config_params)

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)
    balance_value = current_value + cash

    last_loop = get_json(sub_path + last_loop_path)
    last_timestamp = last_loop['timestamp']
    last_signal_timestamp = last_loop['signal_timestamp']
    close_price = last_loop['close_price']
    signal_price = last_loop['signal_price']

    text += f'\nBalance: {balance_value:.2f} {quote_currency}'
    text += f'\nCurrent value: {current_value:.2f} {quote_currency}'
    text += f'\nCash: {cash:.2f} {quote_currency}'
    text += f'\nLast timestamp: {last_signal_timestamp}'
    text += f'\nClose price: {close_price:.2f} {quote_currency}'
    text += f'\nSignal price: {signal_price:.2f} {quote_currency}'

    position = get_current_position(exchange, config_params)

    if position != None:
        side = position['side']
        realised = float(position['realizedPnl'])
        entry_price = float(position['entryPrice'])
        liquidate_price = float(position['estimatedLiquidationPrice'])
        max_drawdown = last_loop['max_drawdown']

        if position['side'] == 'buy':
            drawdown = max(1 - (last_price / entry_price), 0)
        elif position['side'] == 'sell':
            drawdown = max((last_price / entry_price) - 1, 0)
        
        text += f'\nSide: {side}'
        text += f'\nRealise: {realised}'
        text += f'\nLast price: {last_price} {quote_currency}'
        text += f'\nEntry price: {entry_price} {quote_currency}'
        text += f'\nLiquidate price: {liquidate_price}'
        text += f'\nDrawdown: {drawdown * 100:.2f}%'
        text += f'\nMax drawdown: {max_drawdown * 100:.2f}%'
    else:
        text += '\nNo open position'

    text += f'\n\nLast active: {last_timestamp}'
    
    return text