import ccxt
import pandas as pd
import datetime as dt
import sys

from func_get import get_time, get_date, get_bid_price, get_ask_price, get_balance, append_cash_flow_df
from func_noti import line_send


def append_df(df_path, order, symbol, amount_key):
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


def remove_df(df_path, order_id):
    df = pd.read_csv(df_path)

    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop = True)
    df.to_csv(df_path, index = False)


def append_error_log(error_log, error_log_df_path):
    df = pd.read_csv(error_log_df_path)
    
    timestamp = get_time()
    df.loc[len(df)] = [timestamp, error_log]
    df.to_csv(error_log_df_path, index = False)


def append_profit(sell_order, symbol, exe_amount, queque_df, profit_df_path):
    df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    buy_id = queque_df['order_id'][len(queque_df) - 1]
    sell_id = sell_order['id']
    buy_price = queque_df['price'][len(queque_df) - 1]
    sell_price = sell_order['price']
    profit = (sell_price - buy_price) * exe_amount

    df.loc[len(df)] = [timestamp, buy_id, sell_id, symbol, exe_amount, buy_price, sell_price, profit]
    df.to_csv(profit_df_path, index = False)


def update_profit(sell_order, symbol, queue_df_path, profit_df_path, amount_key):
    queque_df = pd.read_csv(queue_df_path)
    sell_amount = sell_order[amount_key]

    while sell_amount > 0:
        sell_queue = queque_df['amount'][len(queque_df) - 1]
        exe_amount = min(sell_amount, sell_queue)
        remaining_queue = sell_queue - exe_amount

        append_profit(sell_order, symbol, exe_amount, queque_df, profit_df_path)
        
        if remaining_queue == 0:
            queque_df = queque_df.drop([len(queque_df) - 1])
        else:
            queque_df.loc[len(queque_df) - 1, 'amount'] = remaining_queue

        queque_df.to_csv(queue_df_path, index = False)
        sell_amount -= exe_amount


def noti_success_order(bot_name, order, symbol, base_currency, quote_currency):
    message = '{}: {} {:.3f} {} at {:.2f} {}'.format(bot_name, order['side'], order['filled'], base_currency, order['price'], quote_currency)
    line_send(message)
    print(message)


def cancel_open_order(exchange, order_id, symbol, open_orders_df_path, error_log_df_path):
    try:
        exchange.cancel_order(order_id, symbol)
        remove_df(open_orders_df_path, order_id)
        print('Cancel order {}'.format(order_id))
        cont_flag = 1
    except ccxt.OrderNotFound:
        # no order in the system (could casued by the order is queued), skip for the next loop
        append_error_log('OrderNotFound', error_log_df_path)
        print('Error: Cannot cancel order {}, wait for the next loop'.format(order_id))
        cont_flag = 0
    except ccxt.InvalidOrder:
        # the order is closed by system (could caused by post_only param for buy orders)
        remove_df(open_orders_df_path, order_id)
        cont_flag = 1

    return cont_flag


def check_open_orders(exchange, bot_name, symbol, base_currency, quote_currency, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path):
    cont_flag = 1
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) == 1:
        # 1 order at most
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, symbol)

        if order['filled'] > 0:
            if order['status'] != 'closed':
                cont_flag = cancel_open_order(exchange, order_id, symbol, open_orders_df_path, error_log_df_path)
            
            remove_df(open_orders_df_path, order_id)
            append_df(transactions_df_path, order, symbol, amount_key = 'filled')
            noti_success_order(bot_name, order, symbol, base_currency, quote_currency)
        
            if order['side'] == 'buy':
                append_df(queue_df_path, order, symbol, amount_key = 'filled')
            elif order['side'] == 'sell':
                update_profit(order, symbol, queue_df_path, profit_df_path, amount_key = 'filled')
        
        else:
            cont_flag = cancel_open_order(exchange, order_id, symbol, open_orders_df_path, error_log_df_path)            

    return cont_flag


def rebalance(exchange, current_value, symbol, base_currency, quote_currency, fix_value, min_value, last_price, open_orders_df_path, error_log_df_path):
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
            order = exchange.create_order(symbol, 'limit', side, amount, price)
            append_df(open_orders_df_path, order, symbol, amount_key = 'amount')
            print('Open {} {:.3f} {} at {:.2f} {}'.format(side, amount, base_currency, price, quote_currency))
        except ccxt.InsufficientFunds: 
            # not enough fund (could caused by wrong account), stop the process
            append_error_log('InsufficientFunds', error_log_df_path)
            print('Error: Cannot {} at price {:.2f} {} due to insufficient fund!!!'.format(side, price, quote_currency))
            sys.exit(1)


def update_cash_flow(exchange, bot_name, symbol, fix_value, current_value, last_price, transactions_df_path, profit_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transactions_df = pd.read_scv(transactions_df_path)

    try:
        last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
        last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    except IndexError:
        last_date = None

    cur_date = get_date()
    prev_date = cur_date - dt.timedelta(days = 1)
    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    # skip 1st date
    if (len(last_transactions_df) > 0) | (len(cash_flow_df) > 0):
        if last_date != cur_date:
            balance = get_balance(exchange, symbol, last_price)
            cash = balance - current_value
            
            profit_df = pd.read_csv(profit_df_path)
            last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
            cash_flow = sum(last_profit_df['profit'])
            
            append_cash_flow_df(prev_date, balance, cash_flow, cash, cash_flow_df, cash_flow_df_path)