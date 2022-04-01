import math
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
    

def get_ohlcv():
    '''
    Fetch candle stick from exchange at the longest windows signal.
    '''
    ohlcv_df = None
    
    return ohlcv_df


def get_signals():
    '''
    Conclude action for each in a timestamp.
    '''
    signal = None
    
    return signal


def check_close_signals():
    '''
    Close position if one of criteria occur.
    Return close_position flag.
    '''
    action_flag = None

    return action_flag


def check_open_signals():
    '''
    Open position if all criteria are met.
    Return action whether it is "sell" "buy" or "no action" needed.
    '''
    ohlcv_df = get_ohlcv()
    ohlcv_signal_df = get_signals(ohlcv_df)
    
    action = None

    return action


def get_trigger_condition(order_type, config_params_path):
    '''
    '''
    config_params = get_json(config_params_path)

    if order_type == 'sl':
        checked_condition = config_params['sl']
    elif order_type == 'tp':
        checked_condition = config_params['tp']
    else:
        raise ValueError('order type is invalid!!!')

    for k in checked_condition.keys():
        trigger_condition = checked_condition[k]
        if trigger_condition is not None:
            break

    return trigger_condition


def cal_technical_trigger_price(price, price_condition, order_type, action, decimals, trigger_only):
    '''
    Calculate trigger price and order price based on price conditions.
    '''
    if price_condition.keys()[0] == 'price_percent':
        price_changes = price_condition['price_percent'] / 100

        if (action == 'buy' and order_type == 'stop') or (action == 'sell' and order_type == 'takeProfit'): 
            trigger_price = price * (1 - price_changes)
        elif (action == 'sell' and order_type == 'stop') or (action == 'buy' and order_type == 'takeProfit'):
            trigger_price = price * (1 + price_changes)
        else:
            raise ValueError('order type or action is invalid!!!')

    elif price_condition.keys()[0] == 'signal':
        trigger_price = 0
    else:
        raise ValueError('Price condition is not supported!!!')

    if trigger_only:
        return trigger_price
    else:
        if trigger_price < price:
            order_price = math.floor(trigger_price * decimals) / decimals
        else:
            order_price = math.ceil(trigger_price * decimals) / decimals

        return trigger_price, order_price


def create_trigger_action(action, order_type):
    '''
    Check action and type of order
    Return whether how to place trigger action in advance "buy" or "sell"
    '''
    if (action == 'buy' and order_type == 'stop') or (action == 'buy' and order_type == 'takeProfit'):
        trigger_action = 'sell'
    elif (action == 'sell' and order_type == 'stop') or (action == 'sell' and order_type == 'takeProfit'):
        trigger_action = 'buy'
    else:
        raise ValueError('order type or action is invalid!!!') 

    return trigger_action


def place_order(exchange, symbol, order_type, action, amount, price=None, params={}):
    '''
    Place order.
    '''
    order = exchange.create_order(symbol, order_type, action, amount, price, params)

    return order


def place_trigger_order(exchange, symbol, order_type, action, amount, price_condition, price=None, decimals=1, trigger_only=False):
    '''
    Place trigger order in advance.
    '''
    trigger_action = create_trigger_action(order_type, action)

    if trigger_only:
        trigger_price = cal_technical_trigger_price(price, price_condition, order_type, action, decimals, trigger_only)
        params = {
            'triggerPrice': trigger_price
        }
    else:
        trigger_price, order_price = cal_technical_trigger_price(price, price_condition, order_type, action, decimals, trigger_only)
        params = {
            'triggerPrice': trigger_price, 
            'orderPrice': order_price
        }

    trigger_order = place_order(exchange, symbol, order_type, trigger_action, amount, params=params)

    return trigger_order


def cal_technical_budget(total_budget, n_position, leverage):
    '''
    Find budget per number of positions that can be opened and
    If not a future market leverage should be set to 1. 
    '''
    budget = (total_budget / n_position) * leverage

    return budget


def find_n_remaining(symbols, config_params_path):
    '''
    Find maximum number of remaining can be open.
    '''
    config_params = get_json(config_params_path)
    n_orders = config_params['risk'] * 100
    n_symbols = len(symbols)

    n_remaining = min(n_orders, n_symbols)

    return n_remaining


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


def get_no_position_symbols(config_params_path, last_loop_path):
    '''
    Return list of coins with no position.
    '''
    config_params = get_json(config_params_path)
    last_loop = get_json(last_loop_path)

    all_symbols = config_params['symbols']
    opened_position_symbols = list(last_loop['symbols'].keys())

    no_position_symbols = [symbol for symbol in all_symbols if symbol not in opened_position_symbols]

    return no_position_symbols 


def close_position(exchange, symbol, action, last_loop_path):
    '''
    Create order to close position.
    '''
    last_loop = get_json(last_loop_path)

    amount = last_loop['symbols'][symbol]
    order = exchange.create_order(symbol, 'market', action, amount)

    return order


def open_position(exchange, symbol, action, n_positions, config_params_path):
    '''
    Create order to open position.
    '''
    config_params = get_json(config_params_path)

    total_budget = config_params['total_budget']
    leverage = config_params['leverage']
    budget = cal_technical_budget(total_budget, n_positions, leverage)

    last_price = get_last_price(exchange, symbol)
    amount = budget / last_price

    order = place_order(exchange, symbol, 'market', action, amount)
    
    sl_condition = get_trigger_condition('sl', config_params_path)
    tp_condition = get_trigger_condition('tp', config_params_path)

    sl_order = place_trigger_order(exchange, symbol, 'stop', action, amount, sl_condition, last_price, trigger_only=False)
    tp_order = place_trigger_order(exchange, symbol, 'takeProfit', action, amount, tp_condition, last_price, trigger_only=False)

    return order, sl_order, tp_order 


def create_action(position, close_flag=True):
    '''
    Check current position and purpose of the action
    Return whether "buy" or "sell" action
    '''
    if (position == 'long' & close_flag == True) | (position == 'short' & close_flag == False):
        action = 'sell'
    elif (position == 'short' & close_flag == True)  (position == 'long' & close_flag == False):
        action = 'buy'
    else:
        raise ValueError('position should be long or short only!!!')

    return action


def clear_position_symbol(symbol, last_loop_dict):
    '''
    Remove position of symbol before entering
    a new position.
    '''
    last_loop_dict.pop(symbol)


def update_last_loop(exchange, config_system, order, last_loop_path):
    '''
    Update position amount and price for each symbol after opening the position.
    '''
    last_loop = get_json(last_loop_path)

    symbol = order['symbol']
    side = order['side']
    amount = order['amount']

    fee, _ = get_order_fee(order, exchange, symbol, config_system)
    open_price = cal_adjusted_price(order, fee, side)

    last_loop['symbols'][symbol] = {
        'side': side, 
        'amount': amount, 
        'open_price': open_price 
    }

    update_json(last_loop, last_loop_path)


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
    

def check_close_position(exchange, config_system, config_params_path, last_loop_path, transactions_df_path, profit_df_path):
    '''
    For each symbol opened position in last loop
    If that symbol automatically close by trigger orders
        Fetch all trigger orders
        Keep success orders as transactions
        Record profit
        Update budget from profit
    Otherwise
        Check close signals
        Close positions if matched
        Keep success orders as transactions
        Record profit
        Update budget from profit
    Remove symbol from last loop
    '''
    last_loop = get_json(last_loop_path)
    symbols = list(last_loop['symbols'].keys())
    
    for symbol in symbols:
        last_loop_amount = last_loop[symbol]['amount']
        balance_amount = get_base_currency_amount(exchange, symbol)

        if balance_amount != last_loop_amount:
            orders = get_stop_orders(exchange, symbol, last_loop_path)

            net_profit = 0
            for order in orders:
                append_order(order, 'filled', 'stop_position', transactions_df_path)
                append_technical_profit(exchange, order, config_system, last_loop, profit_df_path)

                net_profit += cal_technical_profit(exchange, order, config_system, last_loop)

            update_technical_budget(net_profit, config_params_path)
        else:
            position = symbol['position']
            action_flag = check_close_signals()
                
            if action_flag == True:
                action = create_action(position, close_flag=True)
                order = close_position(exchange, symbol, action, last_loop_path)

                append_order(order, 'filled', 'close_position', transactions_df_path)    
                append_technical_profit(exchange, order, config_system, last_loop, profit_df_path)

                net_profit = cal_technical_profit(exchange, order, config_system, last_loop)
                update_technical_budget(net_profit, config_params_path)

        clear_position_symbol(symbol, last_loop)


def check_open_position(exchange, config_system, config_params_path, last_loop_path, transactions_df_path):
    '''    
    Find maximum number of positions that can be opened
    Each coin, Check how many positions can be opened?
        Check long position first then short positon
        Open order
        Save transaction
        Update positions
    '''
    no_position_symbols = get_no_position_symbols(config_params_path, last_loop_path)
    n_remaining = find_n_remaining(no_position_symbols, config_params_path)

    for n in range(n_remaining):
        symbol = no_position_symbols[n]
        action = check_open_signals()

        if action != "no action":
            order, _, _ = open_position(exchange, symbol, action, n_remaining, config_params_path)

            append_order(order, 'filled', 'open_position', transactions_df_path)
            update_last_loop(exchange, config_system, order, last_loop_path)

        else:
            print(f"No action on {symbol}")


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