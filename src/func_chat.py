import pandas as pd

import func_get
import func_rebalance
import func_grid
import func_technical


def get_balance_text(bot_list, config_system_path):
    text = "Balance\n"

    config_system = func_get.get_json(config_system_path)
    exchange = func_get.get_exchange(config_system)
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


def get_reserve_text(home_path, bot_list, transfer_path, cash_flow_df_path):
    text = "Reserve\n"
    
    all_reserve = 0
    reserve_dict = {}
    
    for bot_name in bot_list:
        bot_path = f"{home_path}{bot_name}/"

        transfer = func_get.get_json(bot_path + transfer_path)
        cash_flow_df = pd.read_csv(bot_path + cash_flow_df_path)
        reserve = func_get.get_reserve(transfer, cash_flow_df)
        reserve_dict[bot_name] = reserve

        all_reserve += reserve

    for bot_name in reserve_dict:
        text += f"\n{bot_name} Reserve: {reserve_dict[bot_name]} USD"

    text += f"\n\nAll reserve: {all_reserve} USD"

    return text


def get_rebalance_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path):
    bot_path = f"{home_path}{bot_name}/"
    text = f"{bot_name.upper()}\n{bot_type.title()}\n"

    config_system = func_get.get_json(bot_path + config_system_path)
    config_params = func_get.get_json(bot_path + config_params_path)
    transfer = func_get.get_json(bot_path + transfer_path)
    cash_flow_df = pd.read_csv(bot_path + cash_flow_df_path)

    exchange = func_get.get_exchange(config_system)
    symbol_list = list(config_params['symbol'])

    total_value, value_dict = func_rebalance.get_total_value(exchange, config_params)
    cash = func_get.get_quote_currency_value(exchange, symbol_list[0])
    balance_value = total_value + cash
    reserve = func_get.get_reserve(transfer, cash_flow_df)

    cur_date = func_get.get_date()
    cash_flow = func_rebalance.get_cash_flow_rebalance(cur_date, bot_path + profit_df_path)
    funding_payment, funding_dict = func_get.get_funding_payment(exchange, range='today')

    last_loop = func_get.get_json(bot_path + last_loop_path)

    text += f"\nBalance: {balance_value} USD"

    for symbol in value_dict:
        last_price = func_get.get_last_price(exchange, symbol)

        text += f"\n\n{symbol}"
        text += f"\n   Last price: {last_price} USD"
        text += f"\n   Average cost: {last_loop['symbol'][symbol]['average_cost']} USD"
        text += f"\n   Current value: {value_dict[symbol]['current_value']} USD"
    
    text += f"\n\nCash: {cash} USD"
    text += f"\Reserve: {reserve} USD"
    text += f"\n\nCash flow: {cash_flow} USD"
    text += f"\nFunding payment: {funding_payment} USD"
    
    for symbol in funding_dict:
        text += f"\n   {symbol} : {funding_dict[symbol]} USD"

    text += f"\n\nLast active: {last_loop['timestamp']}"
    text += f"\nLast rebalalnce: {last_loop['last_rebalance_timestamp']}"
    text += f"\nNext rebalance: {last_loop['next_rebalance_timestamp']}"

    return text


def get_grid_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path):
    bot_path = f"{home_path}{bot_name}/"
    text = f"{bot_name.upper()}\n{bot_type.title()}\n"

    config_system = func_get.get_json(bot_path + config_system_path)
    config_params = func_get.get_json(bot_path + config_params_path)
    transfer = func_get.get_json(bot_path + transfer_path)
    cash_flow_df = pd.read_csv(bot_path + cash_flow_df_path)

    exchange = func_get.get_exchange(config_system)
    base_currency, quote_currency = func_get.get_currency(config_params['symbol'])
    last_price = func_get.get_last_price(exchange, config_params['symbol'])

    if '-PERP' in config_params['symbol']:
        current_value = 0
    else:
        current_value = func_get.get_base_currency_value(last_price, exchange, config_params['symbol'])
    
    cash = func_get.get_quote_currency_value(exchange, config_params['symbol'])
    balance_value = current_value + cash
    reserve = func_get.get_reserve(transfer, cash_flow_df)

    open_orders_df = pd.read_csv(bot_path + open_orders_df_path)
    unrealised, n_open_sell_oders, amount, avg_price = func_grid.cal_unrealised_grid(last_price, config_params['grid'], open_orders_df)
    base_currency_free = func_get.get_base_currency_free(exchange, config_params['symbol'], bot_path + open_orders_df_path)
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = func_get.get_pending_order(bot_path + open_orders_df_path)


    cur_date = func_get.get_date()
    cash_flow = func_grid.get_cash_flow_grid(cur_date, config_params, bot_path + transactions_df_path)
    funding_payment, _ = func_get.get_funding_payment(exchange, range='today')

    last_loop = func_get.get_json(bot_path + last_loop_path)
    
    text += f"\nBalance: {balance_value} {quote_currency}"
    text += f"\nCash: {cash} {quote_currency}"
    text += f"\n\nReserve: {reserve} USD"
    text += f"\nLast price: {last_price} {quote_currency}"
    text += f"\nHold: {amount} {base_currency}"
    text += f"\nOrder: {n_open_sell_oders} orders"
    text += f"\nAverage price: {avg_price} {quote_currency}"
    text += f"\nUntrack: {base_currency_free} {base_currency}"
    text += f"\nUnrealised: {unrealised} {quote_currency}"
    
    text += f"\nCash flow: {cash_flow} {quote_currency}"
    text += f"\nFunding payment: {funding_payment} USD"
    
    text += f"\n\nMin buy price: {min_buy_price} {quote_currency}"
    text += f"\nMax buy price: {max_buy_price} {quote_currency}"
    text += f"\nMin sell price: {min_sell_price} {quote_currency}"
    text += f"\nMax sell price: {max_sell_price} {quote_currency}"

    text += f"\n\nLast active: {last_loop['timestamp']}"
    
    return text


def get_technical_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, position_path, profit_df_path):
    bot_path = f"{home_path}{bot_name}/"
    text = f"{bot_name.upper()}\n{bot_type.title()}\n"

    config_system = func_get.get_json(bot_path + config_system_path)
    config_params = func_get.get_json(bot_path + config_params_path)

    exchange = func_get.get_exchange(config_system, future=True)
    _, quote_currency = func_get.get_currency(config_params['symbol'])
    last_price = func_get.get_last_price(exchange, config_params['symbol'])
    
    balance_value = func_get.get_quote_currency_value(exchange, config_params['symbol'])

    last_loop = func_get.get_json(bot_path + last_loop_path)
    
    cur_date = func_get.get_date()
    profit_df = pd.read_csv(bot_path + profit_df_path)
    today_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == cur_date]
    today_profit = sum(today_profit_df['profit'])

    text += f"\nBalance: {balance_value} {quote_currency}"
    text += f"\nLast timestamp: {last_loop['signal_timestamp']}"
    text += f"\nClose price: {last_loop['close_price']} {quote_currency}"
    text += f"\nSignal price: {last_loop['signal_price']} {quote_currency}"
    
    position = func_get.get_json(bot_path + position_path)

    if position['amount'] > 0:
        position_api = func_get.get_position(exchange, config_params['symbol'])
        liquidate_price = float(position_api['estimatedLiquidationPrice'])
        notional_value = float(position_api['cost'])
        
        unrealised = func_technical.cal_unrealised_technical(last_price, position)
        drawdown = func_technical.cal_drawdown(last_price, position)
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