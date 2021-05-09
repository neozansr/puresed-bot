import datetime as dt
import time

from func_cal import *
from noti import *


def append_df(df, order, symbol):
    timestamp = order['datetime']
    order_id = order['id']
    order_type = order['type']
    order_side = order['side']
    amount = order['amount']
    price = order['price']
    value = amount * price

    df.loc[len(df)] = [timestamp, order_id, symbol, order_type, order_side, amount, price, value]

    return df


def remove_df(df, order_id):
    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop = True)

    return df


def open_sell_order(exchange, order, symbol, sell_price, fee_percent):
    amount = order['amount']
    final_amount = cal_fee(amount, fee_percent)
    sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
    print('Open sell {} {} at {} USDT'.format(final_amount, symbol.split('/')[0], sell_price))

    return sell_order


def noti_success_order(order):
    message = '{} {} at {} USDT'.format(order['side'], order['amount'], order['price'])
    line_send(message)
    print(message)


def check_orders_statue(exchange, side, symbol, grid, latest_price, fee_percent, open_orders_df, transactions_df):
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        if order['status'] == 'closed':
            order_id = order['id']
            noti_success_order(order)
            open_orders_df = remove_df(open_orders_df, order_id)
            transactions_df = append_df(transactions_df, order, symbol)

            # open sell orders after buy orders filled
            if side == 'buy':
                sell_price = cal_sell_price(order, grid, latest_price)
                sell_order = open_sell_order(exchange, order, symbol, sell_price, fee_percent)
                open_orders_df = append_df(open_orders_df, sell_order, symbol)

    return open_orders_df, transactions_df


def cancel_open_buy_orders(exchange, symbol, open_orders_df):
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()

    for order_id in open_buy_orders_list:
        cancel_order = exchange.cancel_order(order_id, symbol)
        order_id = cancel_order['data']['cancelledOrderIds'][0]
        print('Cancel order {}'.format(order_id))
        open_orders_df = remove_df(open_orders_df, order_id)

    return open_orders_df


def open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, latest_price, min_price, max_price, open_orders_df, transactions_df):
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    try:
        max_open_buy_price = max(open_buy_orders_df['price'])
    except ValueError: # for 0 open orders
        max_open_buy_price = 0

    if latest_price - max_open_buy_price > grid:
        open_orders_df = cancel_open_buy_orders(exchange, symbol, open_orders_df)
        buy_price_list = cal_new_orders(n_order, n_sell_order, grid, latest_price)
    else:
        buy_price_list = cal_append_orders(n_order, n_open_order, grid, open_buy_orders_df)

    buy_price_list = price_range(buy_price_list, min_price, max_price)

    for price in buy_price_list:
        amount = value / price
        buy_order = exchange.create_order(symbol, 'limit', 'buy', amount, price)
        open_orders_df = append_df(open_orders_df, buy_order, symbol)
        print('Open buy {} {} at {} USDT'.format(amount, symbol.split('/')[0], price))

    return open_orders_df, transactions_df