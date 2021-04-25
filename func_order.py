import datetime as dt


def remove_close_orders(open_orders_list, orders_df, symbol):
    orders_df_clean = orders_df[orders_df['id'].isin(open_orders_list)]
    orders_df_clean = orders_df_clean.reset_index(drop = True)

    return orders_df_clean
    

def cancel_invalid_buy_orders(exchange, orders_df, cancel_orders_price_list):
    # only valid when price rise

    buy_oders_df = orders_df[orders_df['type'] == 'buy']

    buy_oders_df_filt = buy_oders_df[buy_oders_df['price'].isin(cancel_orders_price_list)]
    cancel_buy_orders_list = buy_oders_df_filt['id'].to_list()

    for order in cancel_buy_orders_list:
        exchange.cancel_order('{}'.format(order))

    orders_df_clean = orders_df[~orders_df['id'].isin(cancel_buy_orders_list)]
    orders_df_clean = orders_df_clean.reset_index(drop = True)

    return orders_df_clean


def update_df(df, order, symbol, value):
    timestamp = dt.datetime.now()
    order_id = order['id']
    order_type = order['order_type']
    side = order['side']
    amount = order['amount']
    price = order['price']

    df.loc[len(df)] = [timestamp, order_id, symbol, order_type, side, amount, price, value]

    return df


def open_buys_order(exchange, orders_df, transcations_df, new_orders_price_list, symbol, grid, value):
    for price in new_orders_price_list:
        amount = value / price
        buy_order = exchange.create_order(symbol, order_type = 'limit', side = 'buy', amount = amount, price = price)
        sell_order = exchange.create_order(symbol, order_type = 'limit', side = 'sell', amount = amount, price = price + grid) # move sell orders to start

        orders_df = update_df(orders_df, buy_order, symbol, value)
        orders_df = update_df(orders_df, sell_order, symbol, value) # move sell orders to start

        transcations_df = update_df(transcations_df, buy_order, symbol, value)
        transcations_df = update_df(transcations_df, sell_order, symbol, value) # move sell orders to start
    
    return orders_df, transcations_df