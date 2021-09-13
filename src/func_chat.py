import pandas as pd

from func_get import get_json, get_date, get_exchange, get_currency, get_currency_future, get_last_price, get_base_currency_value, get_quote_currency_value, get_pending_order, get_available_yield, get_position_api
from func_cal import cal_unrealised, cal_unrealised_future, cal_drawdown_future


def load_available_yield(bot_name, cash_flow_path):
    try:
        cash_flow_df_path = cash_flow_path.format(bot_name)
        cash_flow_df = pd.read_csv(cash_flow_df_path)
        avaialble_yield = get_available_yield(cash_flow_df)
    except FileNotFoundError:
        # Not collect commission fee
        avaialble_yield = 0

    return avaialble_yield


def get_today_yield_rebalance(sub_path, config_params, profit_df_path):
    cur_date = get_date()

    profit_df = pd.read_csv(sub_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]
    cash_flow = sum(today_profit_df['profit'])
    today_yield = cash_flow * config_params['commission_rate']

    return today_yield


def get_today_yield_grid(sub_path, config_params, transactions_df_path):
    cur_date = get_date()

    transactions_df = pd.read_csv(sub_path + transactions_df_path)
    today_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == cur_date]
    today_sell_df = today_transactions_df[today_transactions_df['side'] == 'sell']
    cash_flow = sum(today_sell_df['amount'] * config_params['grid'])
    today_yield = cash_flow * config_params['commission_rate']

    return today_yield


def get_today_yield_technical(sub_path, position_path):
    position = get_json(sub_path + position_path)
    today_yield = position['today_commission']

    return today_yield


def get_balance_text(bot_dict, config_system_path, config_params_path, profit_df_path, transactions_df_path, position_path, cash_flow_path):
    text = "Balance\n"

    config_system = get_json(config_system_path)
    exchange = get_exchange(config_system)
    wallet = exchange.private_get_wallet_all_balances()['result']

    balance_dict = {'account':[], 'asset':[], 'value':[]}

    for s in bot_dict.keys():
        sub_path = f"../{s}/"

        try:
            config_params = get_json(sub_path + config_params_path)
        except FileNotFoundError:
            # Not bot
            pass

        if bot_dict[s] == 'rebalance':
            today_yield = get_today_yield_rebalance(sub_path, config_params, profit_df_path)
        elif bot_dict[s] == 'grid':
            today_yield = get_today_yield_grid(sub_path, config_params, transactions_df_path)
        elif bot_dict[s] == 'technical':
            today_yield = get_today_yield_technical(sub_path, position_path)
        else:
            # Not collect commission fee
            today_yield = 0

        sub_yield = load_available_yield(s, cash_flow_path) + today_yield

        for asset in wallet[s]:
            if float(asset['usdValue']) >= 1:
                balance_dict['account'].append(s)
                balance_dict['asset'].append(asset['coin'])

                sub_value = float(asset['usdValue']) - (sub_yield if asset['coin'] == 'USD' else 0)
                balance_dict['value'].append(sub_value)

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


def get_yield_text(bot_dict, config_params_path, profit_df_path, transactions_df_path, position_path, cash_flow_path):
    text = "Yield\n"
    
    total_yield = 0
    yield_dict = {}
    
    for s in bot_dict.keys():
        sub_path = f"../{s}/"

        try:
            config_params = get_json(sub_path + config_params_path)
        except FileNotFoundError:
            # Not bot
            pass

        if bot_dict[s] == 'rebalance':
            today_yield = get_today_yield_rebalance(sub_path, config_params, profit_df_path)
        elif bot_dict[s] == 'grid':
            today_yield = get_today_yield_grid(sub_path, config_params, transactions_df_path)
        elif bot_dict[s] == 'technical':
            today_yield = get_today_yield_technical(sub_path, position_path)
        else:
            # Not collect commission fee
            today_yield = 0

        sub_yield = load_available_yield(s, cash_flow_path) + today_yield
        total_yield += sub_yield
        yield_dict[s] = sub_yield

    text += f"\nAll Yield: {total_yield:.2f} USD"

    for s in yield_dict.keys():
        text += f"\n{s} Yield: {yield_dict[s]:.2f} USD"

    return text


def get_rebalance_text(bot_name, bot_type, config_system_path, config_params_path, last_loop_path, profit_df_path, cash_flow_path):
    sub_path = f"../{bot_name}/"
    text = f"{bot_name.title()}\n{bot_type.title()}\n"

    config_system = get_json(sub_path + config_system_path)
    config_params = get_json(sub_path + config_params_path)

    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)

    cur_date = get_date()
    profit_df = pd.read_csv(sub_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]
    cash_flow = sum(today_profit_df['profit'])
    today_yield = cash_flow * config_params['commission_rate']
    net_cash_flow = cash_flow - today_yield
    available_yield = load_available_yield(bot_name, cash_flow_path)
    
    balance_value = current_value + cash - available_yield - today_yield

    last_loop = get_json(sub_path + last_loop_path)
    last_timestamp = last_loop['timestamp']

    text += f"\nBalance: {balance_value:.2f} {quote_currency}"
    text += f"\nCurrent value: {current_value:.2f} {quote_currency}"
    text += f"\nCash: {cash:.2f} {quote_currency}"
    text += f"\nToday cash flow: {net_cash_flow:.2f} {quote_currency}"

    text += f"\n\nLast active: {last_timestamp}"

    return text


def get_grid_text(bot_name, bot_type, config_system_path, config_params_path, last_loop_path, open_orders_df_path, transactions_df_path, cash_flow_path):
    sub_path = f"../{bot_name}/"
    text = f"{bot_name.title()}\n{bot_type.title()}\n"

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

    today_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == cur_date]
    today_sell_df = today_transactions_df[today_transactions_df['side'] == 'sell']
    cash_flow = sum(today_sell_df['amount'] * config_params['grid'])
    today_yield = cash_flow * config_params['commission_rate']
    net_cash_flow = cash_flow - today_yield
    available_yield = load_available_yield(bot_name, cash_flow_path)

    balance_value = current_value + cash - available_yield - today_yield
    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised(last_price, config_params, open_orders_df)
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(sub_path + open_orders_df_path)

    last_loop = get_json(sub_path + last_loop_path)
    last_timestamp = last_loop['timestamp']

    text += f"\nBalance: {balance_value:.2f} {quote_currency}"
    text += f"\nHold {amount:.4f} {base_currency} with {n_open_sell_oders} orders at {avg_price:.2f} {quote_currency}"
    text += f"\nUnrealised: {unrealised:.2f} {quote_currency}"
    text += f"\nToday cash flow: {net_cash_flow:.2f} {quote_currency}"
    text += f"\nMin buy price: {min_buy_price:.2f} {quote_currency}"
    text += f"\nMax buy price: {max_buy_price:.2f} {quote_currency}"
    text += f"\nMin sell price: {min_sell_price:.2f} {quote_currency}"
    text += f"\nMax sell price: {max_sell_price:.2f} {quote_currency}"

    text += f"\n\nLast active: {last_timestamp}"

    return text


def get_technical_text(bot_name, bot_type, config_system_path, config_params_path, last_loop_path, position_path, cash_flow_path):
    sub_path = f"../{bot_name}/"
    text = f"{bot_name.title()}\n{bot_type.title()}\n"

    config_system = get_json(sub_path + config_system_path)
    config_params = get_json(sub_path + config_params_path)

    exchange = get_exchange(config_system, future=True)
    _, quote_currency = get_currency_future(config_params)
    last_price = get_last_price(exchange, config_params)

    position = get_json(sub_path + position_path)
    today_net_profit = position['today_profit'] - position['today_commission']
    available_yield = load_available_yield(bot_name, cash_flow_path)

    balance_value = get_quote_currency_value(exchange, quote_currency) - available_yield - position['today_commission']

    last_loop = get_json(sub_path + last_loop_path)
    last_timestamp = last_loop['timestamp']
    last_signal_timestamp = last_loop['signal_timestamp']
    close_price = last_loop['close_price']
    signal_price = last_loop['signal_price']

    text += f"\nBalance: {balance_value:.2f} {quote_currency}"
    text += f"\nLast timestamp: {last_signal_timestamp}"
    text += f"\nClose price: {close_price:.2f} {quote_currency}"
    text += f"\nSignal price: {signal_price:.2f} {quote_currency}"
    
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

    text += f"\nToday profit: {today_net_profit:.2f} {quote_currency}"
    
    text += f"\n\nLast active: {last_timestamp}"
    
    return text