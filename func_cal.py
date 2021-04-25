import numpy as np


def cal_buying_power(budget, orders_df):
    sell_orders_df = orders_df[orders_df['type'] == 'sell']
    hold_value = sell_orders_df['value'].sum()
    buying_power = budget - hold_value

    return buying_power


def cal_buy_orders(latest_price, grid, value, buying_power):
    buy_orders_price_list = []
    n_order = int(np.floor(buying_power / value))
    
    for _ in range(n_order):
        latest_price -= grid
        buy_orders_price_list.append(latest_price)

    return buy_orders_price_list, n_order


def organize_buy_orders(orders_df, buy_orders_price_list, n_order, latest_price, grid):
    buy_oders_df = orders_df[orders_df['type'] == 'buy']
    current_orders_price_list = buy_oders_df['price'].to_list()

    if latest_price > (max(current_orders_price_list) - grid):
        # when price rise, cancel all current orders and send new orders
        cancel_orders_price_list = current_orders_price_list.copy()
        new_orders_price_list = buy_orders_price_list.copy()
    else:
        # when price drop, continue from current orders
        cancel_orders_price_list = [x for x in current_orders_price_list if x > latest_price]
        len_current_orders = len([x for x in current_orders_price_list if x <= latest_price])
        
        new_orders_price_list = []
        buy_price = current_orders_price_list[-1]
        for _ in range(n_order - len_current_orders):
            buy_price -= grid
            new_orders_price_list.append(buy_price)

    return cancel_orders_price_list, new_orders_price_list