import numpy as np
import pandas as pd
import time


def round_down_amount(amount, config_params):
    floor_amount = np.floor((amount * (10 ** config_params['decimal']))) / (10 ** config_params['decimal'])
    
    return floor_amount


def round_up_amount(amount, config_params):
    floor_amount = np.ceil((amount * (10 ** config_params['decimal']))) / (10 ** config_params['decimal'])
    
    return floor_amount


def cal_final_amount(order_id, exchange, base_currency, config_params):
    # Trades can be queried 25 at most.
    # With symbol assigned, trades limit is reduced to 3.
    trades = exchange.fetch_my_trades(limit=25)
    trades_df = pd.DataFrame(trades)

    if len(trades_df) > 0:
        order_trade = trades_df[trades_df['order'] == order_id].reset_index(drop=True)
        
        amount, fee = 0, 0
        
        for i in range(len(order_trade)):
            amount += order_trade['amount'][i]

            if order_trade['fee'][i]['currency'] == base_currency:
                # Only base_currency fee affect sell amount.
                fee += order_trade['fee'][i]['cost']
    else:
        # Unfetchable trades due to surpass 25 limit.
        # Not actual fee, will result base_currency_free due to round down.
        # Fee as maximum rate without uncertain params such as rebase, discount.
        order_fetch = exchange.fetch_order(order_id, config_params['symbol'])
        amount = order_fetch['filled']
        fee = order_fetch['filled'] * (config_params['base_fee'] / 100)

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


def cal_unrealised_future(last_price, position):
    if position['side'] == 'buy':
        margin = last_price - position['entry_price']
    elif position['side'] == 'sell':
        margin = position['entry_price'] - last_price
    
    unrealised = margin * float(position['amount'])

    return unrealised


def cal_drawdown_future(last_price, position):
    if position['side'] == 'buy':
        drawdown = max(1 - (last_price / position['entry_price']), 0)
    elif position['side'] == 'sell':
        drawdown = max((last_price / position['entry_price']) - 1, 0)

    return drawdown