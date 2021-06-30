import ccxt
import pandas as pd
import sys

from func_get import get_time, get_bid_price, get_ask_price
from func_noti import noti_success_order


def append_order(df_path, order, symbol, amount_key):
    df = pd.read_csv(df_path)
    
    timestamp = get_time()
    order_id = order['id']
    order_type = order['type']
    order_side = order['side']
    amount = order[amount_key]
    price = order['price']
    value = amount * price

    df.loc[len(df)] = [timestamp, order_id, symbol, order_type, order_side, amount, price, value]
    df.to_csv(df_path, index = False)


def remove_order(df_path, order_id):
    df = pd.read_csv(df_path)

    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop = True)
    df.to_csv(df_path, index = False)


def append_error_log(error_log, error_log_df_path):
    df = pd.read_csv(error_log_df_path)
    
    timestamp = get_time()
    df.loc[len(df)] = [timestamp, error_log]
    df.to_csv(error_log_df_path, index = False)


def append_profit(sell_order, symbol, exe_amount, queue_df, profit_df_path):
    df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    buy_id = queue_df['order_id'][len(queue_df) - 1]
    sell_id = sell_order['id']
    buy_price = queue_df['price'][len(queue_df) - 1]
    sell_price = sell_order['price']
    profit = (sell_price - buy_price) * exe_amount

    df.loc[len(df)] = [timestamp, buy_id, sell_id, symbol, exe_amount, buy_price, sell_price, profit]
    df.to_csv(profit_df_path, index = False)


def cancel_open_order(exchange, order_id, symbol, open_orders_df_path, error_log_df_path):
    try:
        exchange.cancel_order(order_id, symbol)
        remove_order(open_orders_df_path, order_id)
        print('Cancel order {}'.format(order_id))
        cont_flag = 1
    except ccxt.OrderNotFound:
        # no order in the system (could casued by the order is queued), skip for the next loop
        append_error_log('OrderNotFound', error_log_df_path)
        print('Error: Cannot cancel order {}, wait for the next loop'.format(order_id))
        cont_flag = 0
    except ccxt.InvalidOrder:
        # the order is closed by system (could caused by post_only param for buy orders)
        remove_order(open_orders_df_path, order_id)
        cont_flag = 1

    return cont_flag


def clear_open_order(exchange, order_id, symbol, method, open_orders_df_path, error_log_df_path):
    if method == 'lifo':
        cont_flag = cancel_open_order(exchange, order_id, symbol, open_orders_df_path, error_log_df_path)
    elif method == 'fifo':
        # fifo method have to clear queue wihtin the loop 
        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, symbol)
            cont_flag = 1

    return cont_flag


def update_queue(sell_order, symbol, queue_df_path, profit_df_path, amount_key, method):
    sell_amount = sell_order[amount_key]

    while sell_amount > 0:
        queue_df = pd.read_csv(queue_df_path)
        
        if method == 'fifo':
            order_index = 0
        elif method == 'lifo':
            order_index = len(queue_df) - 1
    
        sell_queue = queue_df['amount'][order_index]
        exe_amount = min(sell_amount, sell_queue)
        remaining_queue = sell_queue - exe_amount

        append_profit(sell_order, symbol, exe_amount, queue_df, profit_df_path)
        
        if remaining_queue == 0:
            queue_df = queue_df.drop([order_index])
        else:
            queue_df.loc[order_index, 'amount'] = remaining_queue

        queue_df.to_csv(queue_df_path, index = False)
        sell_amount -= exe_amount


def check_open_orders(exchange, bot_name, symbol, base_currency, quote_currency, method, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path):
    cont_flag = 1
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, symbol)

        if order['filled'] > 0:
            if order['status'] != 'closed':
                cont_flag = clear_open_order(exchange, order_id, symbol, method, open_orders_df_path, error_log_df_path)

            remove_order(open_orders_df_path, order_id)
            append_order(transactions_df_path, order, symbol, amount_key = 'filled')
            noti_success_order(bot_name, order, symbol, base_currency, quote_currency)
        
            if order['side'] == 'buy':
                append_order(queue_df_path, order, symbol, amount_key = 'filled')
            elif order['side'] == 'sell':
                update_queue(order, symbol, queue_df_path, profit_df_path, amount_key = 'filled', method = method)
        
        else:
            cont_flag = clear_open_order(exchange, order_id, symbol, method, open_orders_df_path, error_log_df_path)

    return cont_flag


def rebalance(exchange, symbol, base_currency, quote_currency, fix_value, current_value, min_value, order_type, open_orders_df_path, error_log_df_path):
    rebalance_flag = 1

    if current_value < fix_value - min_value:
        side = 'buy'
        diff_value = fix_value - current_value
        price = get_bid_price(exchange, symbol)
    elif current_value > fix_value + min_value:
        side = 'sell'
        diff_value = current_value - fix_value
        price = get_ask_price(exchange, symbol)
    else:
        rebalance_flag = 0
        print('No action')
        
    if rebalance_flag == 1:
        amount = diff_value / price
        try:
            if order_type == 'limit':
                order = exchange.create_order(symbol, 'limit', side, amount, price)
            elif order_type == 'market':
                exchange.create_order(symbol, 'market', side, amount)

            append_order(open_orders_df_path, order, symbol, amount_key = 'amount')
            print('Open {} {:.3f} {} at {:.2f} {}'.format(side, amount, base_currency, price, quote_currency))
        except ccxt.InsufficientFunds: 
            # not enough fund (could caused by wrong account), stop the process
            append_error_log('InsufficientFunds', error_log_df_path)
            print('Error: Cannot {} at price {:.2f} {} due to insufficient fund!!!'.format(side, price, quote_currency))
            sys.exit(1)