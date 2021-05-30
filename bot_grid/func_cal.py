import numpy as np
import pandas as pd

from func_get import get_ask_price


def cal_final_amount(exchange, order_id):
    trades_df = pd.DataFrame(exchange.fetch_my_trades())
    order_df = trades_df[trades_df['order'] == order_id].reset_index(drop = True)
    amount = order_df['amount']
    fee = order_df['fee'][0]['cost']
    final_amount = amount - fee

    return final_amount


def cal_sell_price(order, exchange, symbol, grid):
    ask_price = get_ask_price(exchange, symbol)
    buy_price = order['price']
    sell_price = max(buy_price + grid, ask_price)

    return sell_price


def cal_n_order(budget, value, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    n_sell_order = len(open_sell_orders_df)
    n_open_order = len(open_orders_df)
    n_order = int(budget / value)
    
    return n_order, n_sell_order, n_open_order


def cal_new_orders(n_order, n_sell_order, grid, start_price):
    buy_price = start_price

    buy_price_list = []    
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


def cal_realised(value, transactions_df):
    transactions_df['date'] = pd.to_datetime(transactions_df['timestamp']).dt.date
    transactions_sell_df = transactions_df[transactions_df['side'] == 'sell']
    realised = sum(transactions_sell_df['value']) - (value * len(transactions_sell_df))
    
    return realised


def cal_unrealised(grid, last_price, open_orders_df):
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

    unrealised_loss = (last_price - avg_price) * amount

    return unrealised_loss, n_open_sell_oders, amount, avg_price