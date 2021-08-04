import numpy as np
import pandas as pd
import time


def round_down_amount(amount, config_params):
    floor_amount = np.floor((amount * (10 ** config_params['decimal']))) / (10 ** config_params['decimal'])
    
    return floor_amount


def round_up_amount(amount, config_params):
    floor_amount = np.ceil((amount * (10 ** config_params['decimal']))) / (10 ** config_params['decimal'])
    
    return floor_amount


def cal_final_amount(order_id, exchange, config_system, config_params):
    trades_df = pd.DataFrame(exchange.fetch_my_trades(config_params['symbol'], limit=200))
    order_trade = trades_df[trades_df['order'] == order_id].reset_index(drop=True)
    
    amount, fee = 0, 0
    
    for i in range(len(order_trade)):
        amount += order_trade['amount'][i]

        while order_trade['fee'][i] == None:
            # fee is None, wait until updated
            print(f'Wating order {order_id} fee to be updated')
            time.sleep(config_system['idle_stage'])
            
            trades_df = pd.DataFrame(exchange.fetch_my_trades(config_params['symbol'], limit=200))
            order_trade = trades_df[trades_df['order'] == order_id].reset_index(drop=True)
            
        fee += order_trade['fee'][i]['cost']

    deducted_amount = amount - fee
    final_amount = round_down_amount(deducted_amount, config_params)

    return final_amount
    

def cal_unrealised(last_price, config_params, open_orders_df):
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    n_open_sell_oders = len(open_sell_orders_df)
    
    price_list = [x - config_params['grid'] for x in open_sell_orders_df['price']]
    amount_list = open_sell_orders_df['amount'].to_list()

    amount = sum(amount_list)
    total_value = sum([i * j for i, j in zip(price_list, amount_list)])
    
    try:
        avg_price = total_value / amount
    except ZeroDivisionError:
        avg_price = 0

    unrealised = (last_price - avg_price) * amount

    return unrealised, n_open_sell_oders, amount, avg_price