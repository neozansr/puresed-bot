import pandas as pd
import datetime as dt
import time
import sys

from func_get import get_currency, get_bid_price
from func_cal import deduct_fee, cal_sell_price, cal_new_orders, cal_append_orders, price_range
from func_noti import line_send


def append_df(df_path, order, symbol, amount_key):
    df = pd.read_csv(df_path)

    timestamp = dt.datetime.now()
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


def open_sell_order(exchange, buy_order, symbol, grid, maker_fee_percent):
    base_currency, quote_currency = get_currency(symbol)
    sell_price = cal_sell_price(buy_order, exchange, symbol, grid)
    
    try:
        amount = buy_order['filled']
        final_amount = deduct_fee(amount, maker_fee_percent)
        sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
    except: # not available amount to sell (could caused by taker fee), sell free amount
        balance = exchange.fetch_balance()
        base_currency_amount = balance[base_currency]['free']
        sell_order = exchange.create_order(symbol, 'limit', 'sell', base_currency_amount, sell_price)
    
    print('Open sell {} {} at {} {}'.format(final_amount, base_currency, sell_price, quote_currency))
    return sell_order


def noti_success_order(bot_name, order, symbol):
    base_currency, quote_currency = get_currency(symbol)
    message = '{}: {} {} {} at {} {}'.format(bot_name, order['side'], order['filled'], base_currency, order['price'], quote_currency)
    line_send(message)
    print(message)


def check_orders_status(exchange, bot_name, side, symbol, grid, maker_fee_percent, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        if order['status'] == 'closed':
            order_id = order['id']
            noti_success_order(bot_name, order, symbol)

            if side == 'buy':
                sell_order = open_sell_order(exchange, order, symbol, grid, maker_fee_percent)
                append_df(open_orders_df, sell_order, symbol, amount_key = 'amount')

            remove_df(open_orders_df_path, order_id)
            append_df(transactions_df_path, order, symbol, amount_key = 'filled')


def cancel_open_buy_orders(exchange, symbol, grid, maker_fee_percent, open_orders_df_path, transactions_df_path):
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
                append_df(transactions_df_path, order, symbol, amount_key = 'filled')
                sell_order = open_sell_order(exchange, order, symbol, grid, maker_fee_percent)
                append_df(open_orders_df_path, sell_order, symbol, amount_key = 'amount')
            
            remove_df(open_orders_df_path, order_id)
        except: # no order in the system (could casued by the order is queued), skip for the next loop
            print('Error: Cannot cancel order {} due to unavailable order!!!'.format(order_id))


def open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, maker_fee_percent, min_price, max_price, start_safety, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    bid_price = get_bid_price(exchange, symbol)
    max_open_buy_price = max(open_buy_orders_df['price'], default = 0)
    min_open_sell_price = min(open_sell_orders_df['price'], default = 0)

    if bid_price - max_open_buy_price > grid:    
        if len(open_sell_orders_df) == 0:
            start_price = bid_price - (grid * start_safety)
            buy_price_list = cal_new_orders(n_order, n_sell_order, grid, start_price)
        else:
            # grid *2, skip grid to prevent dupplicate order
            start_price = min(bid_price, min_open_sell_price - (grid * 2))
            buy_price_list = cal_new_orders(n_order, n_sell_order, grid, start_price)
            
        if len(open_buy_orders_df) > 0:
            cancel_open_buy_orders(exchange, symbol, grid, maker_fee_percent, open_orders_df_path, transactions_df_path)
    else:
        buy_price_list = cal_append_orders(n_order, n_open_order, grid, open_buy_orders_df)

    buy_price_list = price_range(buy_price_list, min_price, max_price)
    print('Open {} buy orders'.format(len(buy_price_list)))

    base_currency, quote_currency = get_currency(symbol)
    
    for price in buy_price_list:
        amount = value / price
        
        try:
            buy_order = exchange.create_order(symbol, 'limit', 'buy', amount, price)
            append_df(open_orders_df_path, buy_order, symbol, amount_key = 'amount')
            print('Open buy {} {} at {} {}'.format(amount, base_currency, price, quote_currency))
        except: # not enough fund (could caused by wrong account), stop the loop
            print('Error: Cannot buy at price {} {} due to insufficient fund!!!'.format(price, quote_currency))
            sys.exit(1)