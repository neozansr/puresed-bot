import pandas as pd
import datetime as dt
import time
import sys

from func_get import get_coin_name
from func_cal import cal_fee, cal_sell_price, cal_new_orders, cal_append_orders, price_range
from func_noti import line_send


def append_df(df_path, order, symbol):
    df = pd.read_csv(df_path)

    timestamp = dt.datetime.now()
    order_id = order['id']
    order_type = order['type']
    order_side = order['side']
    amount = order['amount']
    price = order['price']
    value = amount * price

    df.loc[len(df)] = [timestamp, order_id, symbol, order_type, order_side, amount, price, value]
    df.to_csv(df_path, index = False)


def remove_df(df_path, order_id):
    df = pd.read_csv(df_path)

    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop = True)
    df.to_csv(df_path, index = False)


def open_sell_order(exchange, buy_order, symbol, sell_price, fee_percent):
    amount = buy_order['filled']
    final_amount = cal_fee(amount, fee_percent)
    sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)

    trade_coin, ref_coin = get_coin_name(symbol)
    print('Open sell {} {} at {} {}'.format(final_amount, trade_coin, sell_price, ref_coin))

    return sell_order


def noti_success_order(bot_name, order, symbol):
    trade_coin, ref_coin = get_coin_name(symbol)
    message = '{}: {} {} {} at {} {}'.format(bot_name, order['side'], order['filled'], trade_coin, order['price'], ref_coin)
    line_send(message)
    print(message)


def check_orders_status(exchange, bot_name, side, symbol, grid, latest_price, fee_percent, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        if order['status'] == 'closed':
            order_id = order['id']
            noti_success_order(bot_name, order, symbol)

            # open sell orders after buy orders filled
            if side == 'buy':
                sell_price = cal_sell_price(order, grid, latest_price)
                sell_order = open_sell_order(exchange, order, symbol, sell_price, fee_percent)
                append_df(open_orders_df, sell_order, symbol)

            remove_df(open_orders_df_path, order_id)
            append_df(transactions_df_path, order, symbol)


def cancel_open_buy_orders(exchange, symbol, grid, latest_price, fee_percent, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()

    for order_id in open_buy_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        filled = order['filled']
        
        try:
            exchange.cancel_order(order_id, symbol)
            print('Cancel order {}'.format(order_id))
            
            if filled > 0:
                append_df(transactions_df_path, order, symbol)
                sell_price = cal_sell_price(order, grid, latest_price)
                sell_order = open_sell_order(exchange, order, symbol, sell_price, fee_percent)
                append_df(open_orders_df_path, sell_order, symbol)
            
            remove_df(open_orders_df_path, order_id)
        except: # no order in the system (could casued by the order is queued), skip for the next loop
            print('Error: Cannot cancel order {} due to unavailable order!!!'.format(order_id))


def open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, latest_price, fee_percent, min_price, max_price, start_safety, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    max_open_buy_price = max(open_buy_orders_df['price'], default = 0)
    min_open_sell_price = min(open_sell_orders_df['price'], default = 0)

    if latest_price - max_open_buy_price > grid:    
        if len(open_sell_orders_df) == 0:
            start_price = latest_price - (grid * start_safety)
            buy_price_list = cal_new_orders(n_order, n_sell_order, grid, start_price)
        else:
            start_price = min(latest_price, min_open_sell_price - grid)
            buy_price_list = cal_new_orders(n_order, n_sell_order, grid, start_price)
            
        if len(open_buy_orders_df) > 0:
            cancel_open_buy_orders(exchange, symbol, grid, latest_price, fee_percent, open_orders_df_path, transactions_df_path)
    else:
        buy_price_list = cal_append_orders(n_order, n_open_order, grid, open_buy_orders_df)

    buy_price_list = price_range(buy_price_list, min_price, max_price)
    print('Open {} buy orders'.format(len(buy_price_list)))

    trade_coin, ref_coin = get_coin_name(symbol)
    
    for price in buy_price_list:
        amount = value / price
        
        try:
            buy_order = exchange.create_order(symbol, 'limit', 'buy', amount, price)
            append_df(open_orders_df_path, buy_order, symbol)
            print('Open buy {} {} at {} {}'.format(amount, trade_coin, price, ref_coin))
        except: # not enough fund (could caused by wrong account), stop the loop
            print('Error: Cannot buy at price {} {} due to insufficient fund!!!'.format(price, ref_coin))
            sys.exit(1)