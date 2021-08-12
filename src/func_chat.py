import pandas as pd

from func_get import get_json, get_date, get_exchange, get_currency, get_currency_future, get_last_price, get_base_currency_value, get_quote_currency_value, get_pending_order, get_position_api
from func_cal import cal_unrealised, cal_unrealised_future, cal_drawdown_future


def get_balance_text(text, config_system_path):
    config_system = get_json(config_system_path)
    
    exchange = get_exchange(config_system)
    wallet = exchange.private_get_wallet_all_balances()['result']
    
    subaccounts = list(wallet.keys())
    subaccounts.sort()

    balance_dict = {'account':[], 'asset':[], 'value':[]}

    for s in subaccounts:
        for asset in wallet[s]:
            if float(asset['usdValue']) >= 1:
                balance_dict['account'].append(s)
                balance_dict['asset'].append(asset['coin'])
                balance_dict['value'].append(float(asset['usdValue']))

    balance_df = pd.DataFrame(balance_dict)

    balance_value = balance_df['value'].sum()
    text += f"\nAll Balance: {balance_value:.2f} USD"

    for sub_account in balance_df['account'].unique():
        sub_account_df = balance_df[balance_df['account'] == sub_account]
        sub_account_value = sub_account_df['value'].sum()
        text += f"\n\nBalance {sub_account}: {sub_account_value:.2f} USD"
        
        for asset in sub_account_df['asset'].unique():
            asset_value = sub_account_df[sub_account_df['asset'] == asset]['value'].sum()
            text += f"\n{sub_account} {asset}: {asset_value:.2f} USD"

    return text


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

    text += f"\nBalance: {balance_value:.2f} {quote_currency}"
    text += f"\nCurrent value: {current_value:.2f} {quote_currency}"
    text += f"\nCash: {cash:.2f} {quote_currency}"
    text += f"\nToday cash flow: {cash_flow:.2f} {quote_currency}"

    text += f"\n\nLast active: {last_timestamp}"

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

    text += f"\nBalance: {balance_value:.2f} {quote_currency}"
    text += f"\nHold {amount:.4f} {base_currency} with {n_open_sell_oders} orders at {avg_price:.2f} {quote_currency}"
    text += f"\nUnrealised: {unrealised:.2f} {quote_currency}"
    text += f"\nToday cash flow: {cash_flow:.2f} {quote_currency}"
    text += f"\nMin buy price: {min_buy_price:.2f} {quote_currency}"
    text += f"\nMax buy price: {max_buy_price:.2f} {quote_currency}"
    text += f"\nMin sell price: {min_sell_price:.2f} {quote_currency}"
    text += f"\nMax sell price: {max_sell_price:.2f} {quote_currency}"

    text += f"\n\nLast active: {last_timestamp}"

    return text


def get_technical_text(text, sub_path, config_system_path, config_params_path, last_loop_path, position_path):
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

    text += f"\nBalance: {balance_value:.2f} {quote_currency}"
    text += f"\nCurrent value: {current_value:.2f} {quote_currency}"
    text += f"\nCash: {cash:.2f} {quote_currency}"
    text += f"\nLast timestamp: {last_signal_timestamp}"
    text += f"\nClose price: {close_price:.2f} {quote_currency}"
    text += f"\nSignal price: {signal_price:.2f} {quote_currency}"

    position = get_json(sub_path + position_path)

    if position['amount'] > 0:
        position_api = get_position_api(exchange, config_params)
        liquidate_price = float(position_api['estimatedLiquidationPrice'])
        notional_value = float(position_api['cost'])
        
        unrealised = cal_unrealised_future(last_price, position)
        drawdown = cal_drawdown_future(last_price, position)
        max_drawdown = last_loop['max_drawdown']
        
        text += f"\nSide: {position['side']}"
        text += f"\nUnrealise: {unrealised:.2f} {quote_currency}"
        text += f"\nLast price: {last_price:.2f} {quote_currency}"
        text += f"\nEntry price: {position['entry_price']:.2f} {quote_currency}"
        text += f"\nLiquidate price: {liquidate_price:.2f}  {quote_currency}"
        text += f"\nNotional value: {notional_value:.2f} {quote_currency}"
        text += f"\nDrawdown: {drawdown * 100:.2f}%"
        text += f"\nMax drawdown: {max_drawdown * 100:.2f}%"
    else:
        text += "\nNo open position"

    text += f"\n\nLast active: {last_timestamp}"
    
    return text