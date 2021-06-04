import ccxt
import numpy as np
import pandas as pd
import datetime as dt
import time
import sys

from func_get import get_currency, update_budget, get_bid_price, get_ask_price, get_last_loop_price, update_last_loop_price, get_cut_loss_flag, update_cut_loss_flag
from func_cal import floor_amount, cal_final_amount, cal_sell_price, cal_new_orders, cal_append_orders
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


def clear_df(df_path):
    df = pd.read_csv(df_path)

    df = df.iloc[0:0]
    df.to_csv(df_path, index = False)


def update_error_log(error_log, error_log_df_path):
    df = pd.read_csv(error_log_df_path)
    
    timestamp = dt.datetime.now()
    df.loc[len(df)] = [timestamp, error_log]
    df.to_csv(error_log_df_path, index = False)


def open_sell_order(exchange, buy_order, symbol, grid, decimal, error_log_df_path):
    base_currency, quote_currency = get_currency(symbol)
    ask_price = get_ask_price(exchange, symbol)
    sell_price = cal_sell_price(buy_order, ask_price, grid)
    
    try:
        final_amount = cal_final_amount(exchange, buy_order['id'], decimal)
        sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
    except ccxt.InsufficientFunds:
        # not available amount to sell (could caused by decimal), sell free amount
        balance = exchange.fetch_balance()
        base_currency_amount = balance[base_currency]['free']
        final_amount = floor_amount(base_currency_amount, decimal)
        sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
        update_error_log('InsufficientFunds', error_log_df_path)
    
    print('Open sell {:.3f} {} at {:.2f} {}'.format(final_amount, base_currency, sell_price, quote_currency))
    return sell_order


def noti_success_order(bot_name, order, symbol):
    base_currency, quote_currency = get_currency(symbol)
    message = '{}: {} {:.3f} {} at {:.2f} {}'.format(bot_name, order['side'], order['filled'], base_currency, order['price'], quote_currency)
    line_send(message)
    print(message)


def noti_warning(bot_name, warning):
    message = '{}: {}!!!!!'.format(bot_name, warning)
    line_send(message)
    print(message)


def check_orders_status(exchange, bot_name, side, symbol, grid, decimal, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        if order['status'] == 'closed':
            noti_success_order(bot_name, order, symbol)

            if side == 'buy':
                sell_order = open_sell_order(exchange, order, symbol, grid, decimal, error_log_df_path)
                append_df(open_orders_df_path, sell_order, symbol, amount_key = 'amount')

            remove_df(open_orders_df_path, order_id)
            append_df(transactions_df_path, order, symbol, amount_key = 'filled')

        elif order['status'] == 'canceled':
            # Canceld by param PostOnly
            remove_df(open_orders_df_path, order_id)



def cancel_open_buy_orders(exchange, symbol, grid, decimal, sell_filled, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()
    
    if len(open_buy_orders_list) > 0:
        for order_id in open_buy_orders_list:
            order = exchange.fetch_order(order_id, symbol)
            filled = order['filled']
            
            try:
                exchange.cancel_order(order_id, symbol)
                print('Cancel order {}'.format(order_id))
                
                if sell_filled == True:
                    if filled > 0:
                        append_df(transactions_df_path, order, symbol, amount_key = 'filled')
                        sell_order = open_sell_order(exchange, order, symbol, grid, decimal, error_log_df_path)
                        append_df(open_orders_df_path, sell_order, symbol, amount_key = 'amount')
                
                remove_df(open_orders_df_path, order_id)
            except ccxt.OrderNotFound:
                # no order in the system (could casued by the order is queued), skip for the next loop
                update_error_log('OrderNotFound', error_log_df_path)
                print('Error: Cannot cancel order {} due to unavailable order!!!'.format(order_id))
            except ccxt.InvalidOrder:
                # the order is closed by system (could caused by post_only param for buy orders)
                remove_df(open_orders_df_path, order_id)



def open_buy_orders(exchange, n_order, n_sell_order, n_open_order, symbol, grid, value, start_safety, decimal, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    bid_price = get_bid_price(exchange, symbol)
    max_open_buy_price = max(open_buy_orders_df['price'], default = 0)
    min_open_sell_price = min(open_sell_orders_df['price'], default = np.inf)

    if min(bid_price, min_open_sell_price - grid) - max_open_buy_price > grid:    
        if len(open_sell_orders_df) == 0:
            start_price = bid_price - (grid * start_safety)
            buy_price_list = cal_new_orders(n_order, n_sell_order, grid, start_price)
        else:
            # grid * 2, skip grid to prevent dupplicate order
            start_price = min(bid_price, min_open_sell_price - (grid * 2))
            buy_price_list = cal_new_orders(n_order, n_sell_order, grid, start_price)
            
        cancel_open_buy_orders(exchange, symbol, grid, decimal, True, open_orders_df_path, transactions_df_path, error_log_df_path)
    else:
        buy_price_list = cal_append_orders(n_order, n_open_order, grid, open_buy_orders_df)

    print('Open {} buy orders'.format(len(buy_price_list)))

    base_currency, quote_currency = get_currency(symbol)
    
    for price in buy_price_list:
        amount = value / price
        final_amount = floor_amount(amount, decimal)
        
        try:
            buy_order = exchange.create_order(symbol, 'limit', 'buy', final_amount, price, params = {'postOnly':True})
            append_df(open_orders_df_path, buy_order, symbol, amount_key = 'amount')
            print('Open buy {:.3f} {} at {:.2f} {}'.format(amount, base_currency, price, quote_currency))
        except ccxt.InsufficientFunds:
            # not enough fund (could caused by wrong account), stop the process
            update_error_log('InsufficientFunds', error_log_df_path)
            print('Error: Cannot buy at price {:.2f} {} due to insufficient fund!!!'.format(price, quote_currency))
            sys.exit(1)


def check_circuit_breaker(bot_name, exchange, symbol, last_price, circuit_limit, idle_rest, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path):
    cont_flag = 1

    last_loop_price = get_last_loop_price(last_loop_path)
    transactions_df = pd.read_csv(transactions_df_path)

    if len(transactions_df) >= circuit_limit:
        side_list = transactions_df['side'][-circuit_limit:].unique()
        
        if (len(side_list) == 1) & (side_list[0] == 'buy'):
            if last_price <= last_loop_price:
                # Not open sell after cancel orders
                cancel_open_buy_orders(exchange, symbol, 0, 0, False, open_orders_df_path, transactions_df_path, error_log_df_path)
                cont_flag = 0
                update_last_loop_price(exchange, symbol, last_loop_path)
                noti_warning(bot_name, 'Circuit breaker')
                time.sleep(idle_rest)

    return cont_flag


def check_cut_loss(bot_name, exchange, symbol, n_order, last_loop_path, open_orders_df_path, config_params_path):
    cont_flag = 1
    cut_loss_flag = get_cut_loss_flag(last_loop_path)

    base_currency, _ = get_currency(symbol)
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']

    if cut_loss_flag == 0:
        if len(open_sell_orders_df) >= n_order:
            cut_loss_flag = 1

    if cut_loss_flag == 1:
        # if error while cut_loss, keep cut_loss on the next loop 
        update_cut_loss_flag(1, last_loop_path)
        
        exchange.cancel_all_orders()
        clear_df(open_orders_df_path)

        balance = exchange.fetch_balance()
        amount = balance[base_currency]['total']
        exchange.create_order(symbol, 'market', 'sell', amount)
        update_budget(exchange, symbol, config_params_path)
        
        cont_flag = 0
        update_last_loop_price(exchange, symbol, last_loop_path)
        update_cut_loss_flag(0, last_loop_path)
        noti_warning(bot_name, 'Cut loss')

    return cont_flag