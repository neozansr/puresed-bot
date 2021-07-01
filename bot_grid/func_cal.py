import numpy as np
import pandas as pd
import time


def round_down_amount(amount, decimal):
    floor_amount = np.floor((amount * (10 ** decimal))) / (10 ** decimal)
    
    return floor_amount


def cal_final_amount(exchange, order_id, symbol, decimal, idle_stage):
    trades_df = pd.DataFrame(exchange.fetch_my_trades(symbol, limit = 200))
    order_trade = trades_df[trades_df['order'] == order_id].reset_index(drop = True)
    
    amount, fee = 0, 0
    
    for i in range(len(order_trade)):
        amount += order_trade['amount'][i]

        while order_trade['fee'][i] == None:
            # fee is None, wait until updated
            print(f'Wating order {order_id} fee to be updated')
            time.sleep(idle_stage)
            
            trades_df = pd.DataFrame(exchange.fetch_my_trades(symbol, limit = 200))
            order_trade = trades_df[trades_df['order'] == order_id].reset_index(drop = True)
            
        fee += order_trade['fee'][i]['cost']

    deducted_amount = amount - fee
    final_amount = round_down_amount(deducted_amount, decimal)

    return final_amount
    

def cal_sell_price(order, ask_price, grid):
    buy_price = order['price']
    sell_price = max(buy_price + grid, ask_price)

    return sell_price


def cal_budget(budget, grid, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    sell_price_list = open_sell_orders_df['price'].to_list()
    sell_amount_list = open_sell_orders_df['amount'].to_list()
    sell_value_list = [(i - grid) * j for i, j in zip(sell_price_list, sell_amount_list)]
    used_budget = sum(sell_value_list)
    
    # for cal_new_orders
    remain_budget = budget - used_budget
    buy_value_list = open_buy_orders_df['value'].to_list()
    pending_buy_value = sum(buy_value_list)
    
    # for cal_append_orders
    free_budget = remain_budget - pending_buy_value

    return remain_budget, free_budget


def cal_new_orders(remain_budget, grid, value, start_price):
    buy_price = start_price
    remain_n_order = int(remain_budget / np.ceil(value))

    buy_price_list = []    
    
    for _ in range(remain_n_order):
        buy_price_list.append(buy_price)
        buy_price -= grid

    return buy_price_list


def cal_append_orders(free_budget, grid, value, open_buy_orders_df):
    buy_price_list = []
    
    free_n_order = int(free_budget / np.ceil(value))
    min_open_buy_price = min(open_buy_orders_df['price'])
    buy_price = min_open_buy_price - grid

    for _ in range(free_n_order):
        buy_price_list.append(buy_price)
        buy_price -= grid

    return buy_price_list


def cal_buy_price_list(exchange, remain_budget, free_budget, symbol, bid_price, grid, value, start_safety, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    max_open_buy_price = max(open_buy_orders_df['price'], default = 0)
    min_open_sell_price = min(open_sell_orders_df['price'], default = np.inf)

    if min(bid_price, min_open_sell_price - grid) - max_open_buy_price > grid:    
        if len(open_sell_orders_df) == 0:
            start_price = bid_price - (grid * start_safety)
            buy_price_list = cal_new_orders(remain_budget, grid, value, start_price)
        else:
            # grid * 2, skip grid to prevent dupplicate order
            start_price = min(bid_price, min_open_sell_price - (grid * 2))
            buy_price_list = cal_new_orders(remain_budget, grid, value, start_price)
            
        cancel_flag = 1
    else:
        buy_price_list = cal_append_orders(free_budget, grid, value, open_buy_orders_df)
        buy_price_list = []
        cancel_flag = 0

    return buy_price_list, cancel_flag
    

def cal_unrealised(last_price, grid, open_orders_df):
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

    unrealised = (last_price - avg_price) * amount

    return unrealised, n_open_sell_oders, amount, avg_price