import datetime as dt

from func_cal import *
from func_noti import *


def append_df(df, order, symbol):
    timestamp = dt.datetime.now()
    order_id = order['id']
    order_type = order['type']
    order_side = order['side']
    amount = order['amount']
    price = order['price']
    value = amount * price

    df.loc[len(df)] = [timestamp, order_id, symbol, order_type, order_side, amount, price, value]

    return df


def check_open_orders(exchange, symbol, open_orders_df, transaction_df):
    cont_flag = 1
    if len(open_orders_df) == 1:
        order_id = open_orders_df['order_id'][0] # will always be 1 order
        order = exchange.fetch_order(order_id, symbol)

        if order['status'] == 'closed':
            open_orders_df = remove_df(open_orders_df, order_id)
            transactions_df = append_df(transactions_df, open_orders[0], symbol)
        else:
            try:
                exchange.cancel_order(order_id, symbol) # order is in que
            except:
                cont_flag = 0

    return open_orders_df, transactions_df, cont_flag


def noti_success_order(bot_name, order, symbol):
    message = '{}: {} {} {} at {} {}'.format(bot_name, order['side'], order['amount'], symbol.split('/')[0], order['price'], symbol.split('/')[1])
    line_send(message)
    print(message)


def rebalance_port(symbol, fix_value, min_value, latest_price):
    current_value = get_current_value(symbol)

    if current_value > fix_value + min_value:
        side = 'buy'
        amount = cal_buy_amount(current_value, fix_value)
    elif current_value < fix_value - min_value:
        side = 'sell'
        amount = cal_sell_amount(current_value, fix_value)

    buy_order = exchange.create_order(symbol, 'limit', side, amount, latest_price)
    print('Open {} {} {} at {} {}'.format(side, amount, symbol.split('/')[0], price, symbol.split('/')[1]))