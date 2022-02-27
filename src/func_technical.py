import math
from matplotlib.pyplot import close
import pandas as pd

from func_get import get_base_currency_value, get_cash_value, get_json, get_time, get_order_fee, get_last_price, get_base_currency_amount
from func_cal import cal_adjusted_price, cal_end_balance, cal_unrealised_future
from func_update import update_json, append_csv, append_order, update_transfer


def get_ohlcv():
    '''
    Fetch candle stick from exchange at the longest windows signal.
    '''
    ohlcv_df = None
    
    return ohlcv_df


def get_signal():
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
    Return position whether it is "short" "long" or "no action" needed.
    '''
    action_flag = None

    return action_flag


def cal_technical_trigger_price(price_conditions, order_type, action, trigger_only):
    '''
    Calculate trigger price and order price based on price conditions.
    '''
    last_price = price_conditions['price']
    price_changes = price_conditions['price_changes']
    decimals = price_conditions['decimals']

    if (action == 'buy' and order_type == 'stop') or (action == 'sell' and order_type == 'takeProfit'): 
        trigger_price = last_price * (1 - price_changes)
    elif (action == 'sell' and order_type == 'stop') or (action == 'buy' and order_type == 'takeProfit'):
        trigger_price = last_price * (1 + price_changes)
    else:
        raise ValueError('order type or action is invalid!!!')

    if trigger_only:
        return trigger_price
    else:
        if trigger_price < last_price:
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


def place_trigger_order(exchange, symbol, order_type, action, amount, price, trigger_only):
    '''
    Place trigger order in advance.
    '''
    price_conditions = {
        'price': price, 
        'price_changes': 0.05, 
        'decimals': 1
    }
    trigger_action = create_trigger_action(order_type, action)

    if trigger_only:
        trigger_price = cal_technical_trigger_price(price_conditions, order_type, action, trigger_only)
        params = {
            'triggerPrice': trigger_price
        }
    else:
        trigger_price, order_price = cal_technical_trigger_price(price_conditions, order_type, action, trigger_only)
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
    stop_loss_order = place_trigger_order(exchange, symbol, 'stop', action, amount, last_price, trigger_only=False)
    take_profit_order = place_trigger_order(exchange, symbol, 'takeProfit', action, amount, last_price, trigger_only=False)

    return order, stop_loss_order, take_profit_order 


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
    position = 'long' if side == 'buy' else 'short'
    amount = order['amount']

    fee = get_order_fee(order, exchange, symbol, config_system)
    open_price = cal_adjusted_price(order, fee, side)

    last_loop['symbols'][symbol] = {
        'position': position, 
        'amount': amount, 
        'open_price': open_price 
    }


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
                append_order(order, 'filled', transactions_df_path)
                append_technical_profit(exchange, order, config_system, last_loop, profit_df_path)

                net_profit += cal_technical_profit(exchange, order, config_system, last_loop)

            update_technical_budget(net_profit, config_params_path)
        else:
            position = symbol['position']
            action_flag = check_close_signals()
                
            if action_flag == True:
                action = create_action(position, close_flag=True)
                order = close_position(exchange, symbol, action, last_loop_path)

                append_order(order, 'filled', transactions_df_path)    
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
        action_flag = check_open_signals()

        if action_flag != "no action":
            action = create_action(action_flag, close_flag=False)
            order = open_position(exchange, symbol, action, n_remaining, config_params_path)

            append_order(order, 'filled', transactions_df_path)
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

    symbols = list(last_loop['symbols'].keys()) 
    
    unrealised = 0
    base_currency_value = 0

    for symbol in symbols:
        last_price = get_last_price(exchange, symbol)
        position = last_loop['symbol']['position'] 
         
        unrealised += cal_unrealised_future(last_price, position)
        base_currency_value += get_base_currency_value(last_price, exchange, symbol)

    cash = get_cash_value(exchange)
    end_balance = cal_end_balance(base_currency_value, cash, transfer)

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