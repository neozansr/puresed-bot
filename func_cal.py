import numpy as np
import pandas as pd


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


def cal_new_orders(n_order, n_sell_order, grid, latest_price, start_market):
    if start_market == True:
        buy_price = latest_price
    else:
        buy_price = latest_price - grid

    buy_price_list = []    
    for _ in range(n_order - n_sell_order):
        buy_price_list.append(buy_price)
        buy_price -= grid

    return buy_price_list


def cal_append_orders_head(n_order, n_open_order, grid, latest_price, open_buy_orders_df):
    buy_price_list = []
    
    max_open_buy_price = max(open_buy_orders_df['price'], default = 0)
    buy_price = max_open_buy_price + grid

    for _ in range(n_order - n_open_order):
        if buy_price <= latest_price: # safety, just in case
            buy_price_list.append(buy_price)
        else:
            break
        
        buy_price += grid

    return buy_price_list


def cal_append_orders_tail(n_order, n_open_order, grid, open_buy_orders_df):
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


def cal_realised(value, transactions_df):
    transactions_df['date'] = pd.to_datetime(transactions_df['timestamp']).dt.date
    transactions_sell_df = transactions_df[transactions_df['side'] == 'sell']
    realised = sum(transactions_sell_df['value']) - (value * len(transactions_sell_df))
    
    return realised


def cal_unrealised(grid, latest_price, open_orders_df):
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    n_open_sell_oders = len(open_sell_orders_df)
    
    price_list = [x - grid for x in open_sell_orders_df['price']]
    amount_list = open_sell_orders_df['amount'].to_list()

    amount = sum(amount_list)
    total_value = sum([i * j for i, j in zip(price_list, amount_list)])
    
    try:
        avg_price = total_value / amount
    except ZeroDivisionError:
        avg_price = 0

    unrealised_loss = (latest_price - avg_price) * amount

    return unrealised_loss, n_open_sell_oders, amount, avg_price