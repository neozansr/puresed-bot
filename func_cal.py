import numpy as np


def cal_fee(amount, fee_percent):
    fee_rate = (fee_percent / 100)
    fee = amount * fee_rate
    final_amount = amount - fee

    return final_amount


def cal_sell_price(order, grid, latest_price):
    buy_price = order['price']
    sell_price = max(buy_price + grid, latest_price)

    return sell_price


def cal_n_order(open_orders_df, budget, value):
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    n_sell_order = len(open_sell_orders_df)
    n_open_order = len(open_orders_df)
    n_order = int(budget / value)
    
    return n_order, n_sell_order, n_open_order


def cal_new_orders(n_order, n_sell_order, grid, latest_price):
    buy_price_list = []
    buy_price = latest_price
    
    for _ in range(n_order - n_sell_order):
        buy_price_list.append(buy_price)
        buy_price -= grid

    return buy_price_list


def cal_append_orders(n_order, n_open_order, grid, open_buy_orders_df):
    buy_price_list = []
    
    min_open_buy_price = min(open_buy_orders_df['price'])
    buy_price = min_open_buy_price - grid

    for _ in range(n_order - n_open_order):
        buy_price_list.append(buy_price)
        buy_price -= grid

    return buy_price_list


def price_range(buy_price_list, min_price, max_price):
    buy_price_list = [x for x in buy_price_list if min_price <= x <= max_price]

    return buy_price_list