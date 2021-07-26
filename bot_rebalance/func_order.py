import ccxt
import pandas as pd
import sys

from func_get import get_time, get_bid_price, get_ask_price
from func_noti import noti_success_order


def append_order(amount_key, order, config_params, df_path):
    df = pd.read_csv(df_path)
    
    timestamp = get_time()
    order_id = order['id']
    order_type = order['type']
    order_side = order['side']
    amount = order[amount_key]
    price = order['price']
    value = amount * price

    df.loc[len(df)] = [timestamp, order_id, config_params['symbol'], order_type, order_side, amount, price, value]
    df.to_csv(df_path, index=False)


def remove_order(order_id, df_path):
    df = pd.read_csv(df_path)

    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop=True)
    df.to_csv(df_path, index=False)


def append_error_log(error_log, error_log_df_path):
    df = pd.read_csv(error_log_df_path)
    
    timestamp = get_time()
    df.loc[len(df)] = [timestamp, error_log]
    df.to_csv(error_log_df_path, index=False)


def append_profit(sell_order, exe_amount, config_params, queue_df, profit_df_path):
    df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    buy_id = queue_df['order_id'][len(queue_df) - 1]
    sell_id = sell_order['id']
    buy_price = queue_df['price'][len(queue_df) - 1]
    sell_price = sell_order['price']
    profit = (sell_price - buy_price) * exe_amount

    df.loc[len(df)] = [timestamp, buy_id, sell_id, config_params['symbol'], exe_amount, buy_price, sell_price, profit]
    df.to_csv(profit_df_path, index=False)


def cancel_open_order(order_id, exchange, config_params, open_orders_df_path, error_log_df_path):
    try:
        exchange.cancel_order(order_id, config_params['symbol'])
        remove_order(order_id, open_orders_df_path)
        print(f'Cancel order {order_id}')
        cont_flag = 1
    except ccxt.OrderNotFound:
        # no order in the system (could casued by the order is queued), skip for the next loop
        append_error_log('OrderNotFound', error_log_df_path)
        print(f'Error: Cannot cancel order {order_id}, wait for the next loop')
        cont_flag = 0
    except ccxt.InvalidOrder:
        # the order is closed by system (could caused by post_only param for buy orders)
        remove_order(open_orders_df_path, order_id)
        cont_flag = 1

    return cont_flag


def clear_open_order(method, order_id, exchange, config_params, open_orders_df_path, error_log_df_path):
    if method == 'lifo':
        cont_flag = cancel_open_order(exchange, order_id, config_params['symbol'], open_orders_df_path, error_log_df_path)
    elif method == 'fifo':
        # fifo method have to clear queue wihtin the loop 
        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, config_params['symbol'])
            cont_flag = 1

    return cont_flag


def update_queue(method, sell_order, amount_key, config_params, queue_df_path, profit_df_path):
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

        append_profit(sell_order, exe_amount, config_params, queue_df, profit_df_path)
        
        if remaining_queue == 0:
            queue_df = queue_df.drop([order_index])
        else:
            queue_df.loc[order_index, 'amount'] = remaining_queue

        queue_df.to_csv(queue_df_path, index=False)
        sell_amount -= exe_amount


def check_open_orders(method, exchange, bot_name, base_currency, quote_currency, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path):
    cont_flag = 1
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, config_params['symbol'])

        if order['filled'] > 0:
            if order['status'] != 'closed':
                cont_flag = clear_open_order(method, order_id, exchange, config_params, open_orders_df_path, error_log_df_path)
        
            if order['side'] == 'buy':
                append_order('filled', order, config_params, queue_df_path)
            elif order['side'] == 'sell':
                update_queue(method, order, 'filled', config_params, queue_df_path, profit_df_path)

            remove_order(order_id, open_orders_df_path)
            append_order('filled', order, config_params, transactions_df_path)
            noti_success_order(order, bot_name, base_currency, quote_currency)
        
        else:
            cont_flag = clear_open_order(method, order_id, exchange, config_params, open_orders_df_path, error_log_df_path)

    return cont_flag


def rebalance(order_type, current_value, exchange, base_currency, quote_currency, config_params, open_orders_df_path, error_log_df_path):
    rebalance_flag = 1

    if current_value < config_params['fix_value'] - config_params['min_value']:
        side = 'buy'
        diff_value = config_params['fix_value'] - current_value
        price = get_bid_price(exchange, config_params)
    elif current_value > config_params['fix_value'] + config_params['min_value']:
        side = 'sell'
        diff_value = current_value - config_params['fix_value']
        price = get_ask_price(exchange, config_params)
    else:
        rebalance_flag = 0
        print('No action')
        
    if rebalance_flag == 1:
        amount = diff_value / price
        try:
            if order_type == 'limit':
                order = exchange.create_order(config_params['symbol'], 'limit', side, amount, price)
            elif order_type == 'market':
                exchange.create_order(config_params['symbol'], 'market', side, amount)

            append_order('amount', order, config_params, open_orders_df_path)
            print(f'Open {side} {amount:.3f} {base_currency} at {price:.2f} {quote_currency}')
        except ccxt.InsufficientFunds: 
            # not enough fund (could caused by wrong account), stop the process
            append_error_log('InsufficientFunds', error_log_df_path)
            print(f'Error: Cannot {side} at price {price:.2f} {quote_currency} due to insufficient fund!!!')
            sys.exit(1)