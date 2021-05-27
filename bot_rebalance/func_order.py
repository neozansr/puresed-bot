import pandas as pd
import datetime as dt

from func_get import get_currency, get_bid_price, get_ask_price, get_current_value
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


def noti_success_order(bot_name, order, symbol):
    base_currency, quote_currency = get_currency(symbol)
    message = '{}: {} {} {} at {} {}'.format(bot_name, order['side'], order['filled'], base_currency, order['price'], quote_currency)
    line_send(message)
    print(message)


def check_open_orders(exchange, bot_name, symbol, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    
    cont_flag = 1
    if len(open_orders_df) == 1:
        order_id = open_orders_df['order_id'][0] # 1 order at most
        order = exchange.fetch_order(order_id, symbol)

        if order['status'] == 'closed':
            remove_df(open_orders_df_path, order_id)
            append_df(transactions_df_path, order, symbol, amount_key = 'filled')
            noti_success_order(bot_name, order, symbol)
        else:
            try: # if the order is pending in server, skip loop
                exchange.cancel_order(order_id, symbol)
                remove_df(open_orders_df_path, order_id)
                print('Cancel order {}'.format(order_id))
            except:
                cont_flag = 0
                print('Error: Cannot cancel order {}, wait for the next loop'.format(order_id))

    return cont_flag


def rebalance_port(exchange, symbol, fix_value, min_value, last_price, open_orders_df_path):
    base_currency, quote_currency = get_currency(symbol)
    current_value = get_current_value(exchange, symbol, last_price)

    rebalance_flag = 1
    if current_value < fix_value - min_value:
        side = 'buy'
        price = get_bid_price(exchange, symbol)
        diff_value = fix_value - current_value
    elif current_value > fix_value + min_value:
        side = 'sell'
        price = get_ask_price(exchange, symbol)
        diff_value = current_value - fix_value
    else:
        rebalance_flag = 0
        print('No action')
        
    if rebalance_flag == 1:
        amount = diff_value / price
        order = exchange.create_order(symbol, 'limit', side, amount, price)
        append_df(open_orders_df_path, order, symbol, amount_key = 'amount')
        print('Open {} {} {} at {} {}'.format(side, amount, base_currency, price, quote_currency))