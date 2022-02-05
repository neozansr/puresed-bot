import pandas as pd

from func_get import get_json, get_date, get_exchange, get_currency, get_last_price, get_base_currency_value, get_cash_value, get_total_value, get_pending_order, get_available_cash_flow, get_position, get_funding_payment
from func_cal import cal_unrealised, cal_unrealised_future, cal_drawdown_future


def get_balance_text(bot_list, config_system_path):
    text = "Balance\n"

    config_system = get_json(config_system_path)
    exchange = get_exchange(config_system)
    wallet = exchange.private_get_wallet_all_balances()['result']

    balance_dict = {'account':[], 'asset':[], 'value':[]}

    for bot_name in bot_list:
        for asset in wallet[bot_name]:
            if float(asset['usdValue']) >= 1:
                balance_dict['account'].append(bot_name)
                balance_dict['asset'].append(asset['coin'])

                sub_value = float(asset['usdValue'])
                balance_dict['value'].append(sub_value)

    balance_df = pd.DataFrame(balance_dict)

    balance_value = balance_df['value'].sum()
    text += f"\nAll Balance: {balance_value} USD"

    for sub_account in balance_df['account'].unique():
        sub_account_df = balance_df[balance_df['account'] == sub_account]
        sub_account_value = sub_account_df['value'].sum()
        text += f"\n\nBalance {sub_account}: {sub_account_value} USD"
        
        for asset in sub_account_df['asset'].unique():
            asset_value = sub_account_df[sub_account_df['asset'] == asset]['value'].sum()
            text += f"\n   {sub_account} {asset}: {asset_value} USD"

    return text


def get_cash_flow_text(home_path, bot_list, transfer_path, cash_flow_df_path):
    text = "Cash flow\n"
    
    all_cash_flow = 0
    cash_flow_dict = {}
    
    for bot_name in bot_list:
        bot_path = f"{home_path}{bot_name}/"

        cash_flow_df = pd.read_csv(bot_path + cash_flow_df_path)
        transfer = get_json(bot_path + transfer_path)
        available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
        cash_flow_dict[bot_name] = available_cash_flow

        all_cash_flow += available_cash_flow

    for bot_name in cash_flow_dict.keys():
        text += f"\n{bot_name} Cash flow: {cash_flow_dict[bot_name]} USD"

    text += f"\n\nAll cash flow: {all_cash_flow} USD"

    return text


def get_rebalance_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, profit_df_path):
    bot_path = f"{home_path}{bot_name}/"
    text = f"{bot_name.upper()}\n{bot_type.title()}\n"

    config_system = get_json(bot_path + config_system_path)
    config_params = get_json(bot_path + config_params_path)

    exchange = get_exchange(config_system)
    total_value, value_dict = get_total_value(exchange, config_params)
    cash = get_cash_value(exchange)
    balance_value = total_value + cash

    cur_date = get_date()
    profit_df = pd.read_csv(bot_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]
    today_cash_flow = sum(today_profit_df['profit'])
    funding_payment, funding_dict = get_funding_payment(exchange, range='today')

    last_loop = get_json(bot_path + last_loop_path)

    text += f"\nBalance: {balance_value} USD"

    for symbol in value_dict.keys():
        last_price = get_last_price(exchange, symbol)

        text += f"\n\n{symbol}"
        text += f"\n   Last price: {last_price} USD"
        text += f"\n   Average cost: {last_loop['symbol'][symbol]['average_cost']} USD"
        text += f"\n   Fix value: {value_dict[symbol]['fix_value']} USD"
        text += f"\n   Current value: {value_dict[symbol]['current_value']} USD"
    
    text += f"\n\nCash: {cash} USD"
    text += f"\nToday cash flow: {today_cash_flow} USD"
    text += f"\nFunding payment: {funding_payment} USD"
    
    for symbol in funding_dict.keys():
        text += f"\n   {symbol} : {funding_dict[symbol]} USD"

    text += f"\n\nLast active: {last_loop['timestamp']}"
    text += f"\nLast rebalalnce: {last_loop['last_rebalance_timestamp']}"
    text += f"\nNext rebalance: {last_loop['next_rebalance_timestamp']}"

    return text


def get_grid_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, open_orders_df_path, transactions_df_path):
    bot_path = f"{home_path}{bot_name}/"
    text = f"{bot_name.upper()}\n{bot_type.title()}\n"

    config_system = get_json(bot_path + config_system_path)
    config_params = get_json(bot_path + config_params_path)

    exchange = get_exchange(config_system)
    base_currency, quote_currency = get_currency(config_params['symbol'])
    last_price = get_last_price(exchange, config_params['symbol'])

    current_value = get_base_currency_value(last_price, exchange, config_params['symbol'])
    cash = get_cash_value(exchange)
    balance_value = current_value + cash

    open_orders_df = pd.read_csv(bot_path + open_orders_df_path)
    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised(last_price, config_params['grid'], open_orders_df)
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(bot_path + open_orders_df_path)

    cur_date = get_date()
    transactions_df = pd.read_csv(bot_path + transactions_df_path)
    today_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == cur_date]
    today_sell_df = today_transactions_df[today_transactions_df['side'] == 'sell']
    today_cash_flow = sum(today_sell_df['amount'] * config_params['grid'])

    last_loop = get_json(bot_path + last_loop_path)
    
    text += f"\nBalance: {balance_value} {quote_currency}"
    text += f"\nCash: {cash} {quote_currency}"
    text += f"\nLast price: {last_price} {quote_currency}"
    text += f"\nHold {amount} {base_currency} with {n_open_sell_oders} orders at {avg_price} {quote_currency}"
    text += f"\nUnrealised: {unrealised} {quote_currency}"
    text += f"\nToday cash flow: {today_cash_flow} {quote_currency}"
    text += f"\nMin buy price: {min_buy_price} {quote_currency}"
    text += f"\nMax buy price: {max_buy_price} {quote_currency}"
    text += f"\nMin sell price: {min_sell_price} {quote_currency}"
    text += f"\nMax sell price: {max_sell_price} {quote_currency}"

    text += f"\n\nLast active: {last_loop['timestamp']}"
    
    return text


def get_technical_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, position_path, profit_df_path):
    bot_path = f"{home_path}{bot_name}/"
    text = f"{bot_name.upper()}\n{bot_type.title()}\n"

    config_system = get_json(bot_path + config_system_path)
    config_params = get_json(bot_path + config_params_path)

    exchange = get_exchange(config_system, future=True)
    _, quote_currency = get_currency(config_params['symbol'])
    last_price = get_last_price(exchange, config_params['symbol'])
    
    balance_value = get_cash_value(exchange)

    last_loop = get_json(bot_path + last_loop_path)
    
    cur_date = get_date()
    profit_df = pd.read_csv(bot_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]
    today_profit = sum(today_profit_df['profit'])

    text += f"\nBalance: {balance_value} {quote_currency}"
    text += f"\nLast timestamp: {last_loop['signal_timestamp']}"
    text += f"\nClose price: {last_loop['close_price']} {quote_currency}"
    text += f"\nSignal price: {last_loop['signal_price']} {quote_currency}"
    
    position = get_json(bot_path + position_path)

    if position['amount'] > 0:
        position_api = get_position(exchange, config_params['symbol'])
        liquidate_price = float(position_api['estimatedLiquidationPrice'])
        notional_value = float(position_api['cost'])
        
        unrealised = cal_unrealised_future(last_price, position)
        drawdown = cal_drawdown_future(last_price, position)
        max_drawdown = last_loop['max_drawdown']
        
        text += f"\nSide: {position['side']}"
        text += f"\nUnrealise: {unrealised} {quote_currency}"
        text += f"\nLast price: {last_price} {quote_currency}"
        text += f"\nEntry price: {position['entry_price']} {quote_currency}"
        text += f"\nLiquidate price: {liquidate_price}  {quote_currency}"
        text += f"\nNotional value: {notional_value} {quote_currency}"
        text += f"\nDrawdown: {drawdown * 100}%"
        text += f"\nMax drawdown: {max_drawdown * 100}%"
    else:
        text += "\nNo open position"

    text += f"\nToday profit: {today_profit} {quote_currency}"
    
    text += f"\n\nLast active: {last_loop['timestamp']}"
    
    return text