import pandas as pd

from func_get import get_json, get_time, get_last_price, get_base_currency_amount, get_base_currency_value, get_quote_currency_value, get_order_fee
from func_cal import cal_adjusted_price
from func_update import update_json, append_csv, append_order, update_transfer


def cal_unrealised_technical(last_price, position):
    '''
    Calculate unrealised balance based on the lastest price.
    '''
    if position['side'] == 'buy':
        margin = last_price - position['entry_price']
    elif position['side'] == 'sell':
        margin = position['entry_price'] - last_price
    
    unrealised = margin * float(position['amount'])

    return unrealised


def cal_drawdown(last_price, position):
    if position['side'] == 'buy':
        drawdown = max(1 - (last_price / position['entry_price']), 0)
    elif position['side'] == 'sell':
        drawdown = max((last_price / position['entry_price']) - 1, 0)

    return drawdown


def cal_technical_profit(exchange, close_order, config_system, last_loop_dict):
    '''
    Calculate profit after closing position
    '''
    fee = get_order_fee(close_order, exchange, close_order['symbol'], config_system)
    open_price = last_loop_dict[close_order['symbol']]['open_price']
    close_price = cal_adjusted_price(close_order, fee, close_order['side'])

    if close_order['side'] == 'sell':
        profit = (close_price - open_price) * close_order['amount']
    elif close_order['side'] == 'buy':
        profit = (open_price - close_price) * close_order['amount']

    return profit


def append_technical_profit(exchange, order, config_system, last_loop_dict, profit_df_path):
    '''
    Calculate profit from close order.
    Record profit on profit file.
    '''
    profit_df = pd.read_csv(profit_df_path)

    timestamp = get_time()
    order_id = order['id']    
    symbol = order['symbol']
    side = order['side']
    amount = order['amount']
    
    fee = get_order_fee(order, exchange, symbol, config_system)
    open_price = last_loop_dict[symbol]['open_price']
    close_price = cal_adjusted_price(order, fee, side)
    profit = cal_technical_profit(exchange, order, config_system, last_loop_dict)    

    profit_df.loc[len(profit_df)] = [timestamp, order_id, symbol, side, amount, open_price, close_price, fee, profit]
    profit_df.to_csv(profit_df_path, index=False)


def update_technical_budget(net_change, config_params_path):
    '''
    Update budget due to net change
        Net transfer
        Net profit
    '''
    config_params = get_json(config_params_path)
    config_params['budget'] += net_change

    update_json(config_params, config_params_path)
    

def get_stop_orders(exchange, symbol, last_loop_path):
    '''
    Get take profit or stop loss order.
    '''
    last_loop = get_json(last_loop_path)
    open_amount = last_loop['amount']

    stop_orders = list()
    trades = exchange.fetch_my_trades(symbol).reverse()

    n_trades = trade_amount = 0
    while trade_amount < open_amount:
        trade = trades[n_trades]
        trade_amount += trade['amount']
        stop_orders.append(trade)

        n_trades += 1

    return stop_orders


def check_close_position():
    '''
    Close positions if meet criteria.
    '''
    pass


def check_open_position():
    '''
    Open positions if meet criteria.
    '''
    pass


def update_end_date_technical(prev_date, exchange, config_params_path, last_loop_path, transfer_path, cash_flow_df_path, profit_df_path):
    '''
    Update cash flow before beginning trading in the next day
        Sum up unrealised for all opened position symbols
        Update end date balance
        Sum up all profit got from the previous day
    Update transfer funding
    Update budget on for trading in the next day
    '''
    config_params = get_json(config_params_path)
    last_loop = get_json(last_loop_path)
    transfer = get_json(transfer_path)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    unrealised = 0
    symbol_list = list(config_params['symbols'].keys())

    for symbol in symbol_list:
        last_price = get_last_price(exchange, symbol)
        position = last_loop['symbol']['position'] 
         
        unrealised += cal_unrealised_technical(last_price, position)

    end_balance = get_quote_currency_value(exchange, symbol_list[0])

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    profit = sum(last_profit_df['profit'])
    
    net_transfer = transfer['deposit'] - transfer['withdraw']

    cash_flow_list = [
        prev_date, 
        end_balance, 
        unrealised, 
        profit, 
        transfer['deposit'],
        transfer['withdraw']
    ]

    append_csv(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_technical_budget(net_transfer, config_params_path)
    update_transfer(config_params['taker_fee'], transfer_path)


def print_position(last_price, position, position_api, quote_currency):
    liquidate_price = float(position_api['estimatedLiquidationPrice'])
    notional_value = float(position_api['cost'])
    unrealised = cal_unrealised_technical(last_price, position)
    drawdown = cal_drawdown(last_price, position)
    
    print(f"Side: {position['side']}")
    print(f"Unrealise: {unrealised} {quote_currency}")
    print(f"Last price: {last_price} {quote_currency}")
    print(f"Entry price: {position['entry_price']} {quote_currency}")
    print(f"Liquidate price: {liquidate_price} {quote_currency}")
    print(f"Notional value: {notional_value} {quote_currency}")
    print(f"Drawdown: {drawdown * 100}%")