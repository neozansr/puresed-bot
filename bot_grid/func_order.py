import ccxt
import numpy as np
import pandas as pd
import datetime as dt
import time
import json
import sys

from func_get import get_time, get_date, get_bid_price, get_ask_price, get_last_loop_price, get_balance, get_greed_index, get_used_cash_flow, get_transfer
from func_get import update_last_loop_price, update_used_cash_flow, update_reinvest, reduce_budget, append_cash_flow_df, reset_used_cash_flow, reset_transfer
from func_cal import floor_amount, cal_final_amount, cal_sell_price, cal_buy_price_list, cal_unrealised
from func_noti import line_send


def append_df(df_path, order, symbol, amount_key):
    df = pd.read_csv(df_path)

    timestamp = get_time()
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


def clear_df(df_path):
    df = pd.read_csv(df_path)

    df = df.iloc[0:0]
    df.to_csv(df_path, index = False)


def update_error_log(error_log, error_log_df_path):
    df = pd.read_csv(error_log_df_path)
    
    timestamp = get_time()
    df.loc[len(df)] = [timestamp, error_log]
    df.to_csv(error_log_df_path, index = False)


def open_sell_order(exchange, buy_order, symbol, base_currency, quote_currency, grid, decimal, idle_stage, error_log_df_path):
    ask_price = get_ask_price(exchange, symbol)
    sell_price = cal_sell_price(buy_order, ask_price, grid)
    
    try:
        final_amount = cal_final_amount(exchange, buy_order['id'], symbol, decimal, idle_stage)
        sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
    except ccxt.InsufficientFunds:
        # not available amount to sell (could caused by decimal), sell free amount
        balance = exchange.fetch_balance()
        base_currency_amount = balance[base_currency]['free']
        final_amount = floor_amount(base_currency_amount, decimal)
        sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
        update_error_log('InsufficientFunds', error_log_df_path)
    except ccxt.InvalidOrder:
        # filled small value than minimum order, ignore
        update_error_log('InvalidOrder', error_log_df_path)
    
    print('Open sell {:.3f} {} at {:.2f} {}'.format(final_amount, base_currency, sell_price, quote_currency))
    return sell_order


def noti_success_order(bot_name, order, symbol, base_currency, quote_currency):
    message = '{}: {} {:.3f} {} at {:.2f} {}'.format(bot_name, order['side'], order['filled'], base_currency, order['price'], quote_currency)
    line_send(message, noti_type = 'order')
    print(message)


def noti_warning(bot_name, warning):
    message = '{}: {}!!!!!'.format(bot_name, warning)
    line_send(message, noti_type = 'warning')
    print(message)


def check_orders_status(exchange, bot_name, side, symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        if order['status'] == 'closed':
            noti_success_order(bot_name, order, symbol, base_currency, quote_currency)

            if side == 'buy':
                sell_order = open_sell_order(exchange, order, symbol, base_currency, quote_currency, grid, decimal, idle_stage, error_log_df_path)
                append_df(open_orders_df_path, sell_order, symbol, amount_key = 'amount')

            remove_df(open_orders_df_path, order_id)
            append_df(transactions_df_path, order, symbol, amount_key = 'filled')

        elif order['status'] == 'canceled':
            # Canceld by param PostOnly
            remove_df(open_orders_df_path, order_id)


def cancel_open_buy_orders(exchange, symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()
    
    if len(open_buy_orders_list) > 0:
        for order_id in open_buy_orders_list:
            order = exchange.fetch_order(order_id, symbol)
            filled = order['filled']
            
            try:
                exchange.cancel_order(order_id, symbol)
                print('Cancel order {}'.format(order_id))
                
                if filled > 0:
                    append_df(transactions_df_path, order, symbol, amount_key = 'filled')
                    sell_order = open_sell_order(exchange, order, symbol, base_currency, quote_currency, grid, decimal, idle_stage, error_log_df_path)
                    append_df(open_orders_df_path, sell_order, symbol, amount_key = 'amount')
                
                remove_df(open_orders_df_path, order_id)
            except ccxt.OrderNotFound:
                # no order in the system (could casued by the order is queued), skip for the next loop
                update_error_log('OrderNotFound', error_log_df_path)
                print('Error: Cannot cancel order {} due to unavailable order!!!'.format(order_id))
            except ccxt.InvalidOrder:
                # the order is closed by system (could caused by post_only param for buy orders)
                remove_df(open_orders_df_path, order_id)


def open_buy_orders(exchange, bot_name, remain_budget, free_budget, symbol, base_currency, quote_currency, grid, value, start_safety, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    bid_price = get_bid_price(exchange, symbol)
    buy_price_list, cancel_flag = cal_buy_price_list(exchange, remain_budget, free_budget, symbol, bid_price, grid, value, start_safety, open_orders_df_path)
    
    if cancel_flag == 1:
        cancel_open_buy_orders(exchange, symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)

    print('Open {} buy orders'.format(len(buy_price_list)))

    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    remain_cash_flow_accum = sum(cash_flow_df['remain_cash_flow'])

    for price in buy_price_list:
        amount = value / price
        final_amount = floor_amount(amount, decimal)
        
        try:
            balance = exchange.fetch_balance()
            quote_currency_amount = balance[quote_currency]['free']

            # ignore ccxt.InsufficientFunds until cal_budget can be resolved
            if quote_currency_amount >= remain_cash_flow_accum + value:
                buy_order = exchange.create_order(symbol, 'limit', 'buy', final_amount, price, params = {'postOnly':True})
                append_df(open_orders_df_path, buy_order, symbol, amount_key = 'amount')
                print('Open buy {:.3f} {} at {:.2f} {}'.format(amount, base_currency, price, quote_currency))
            else:
                update_error_log('InsufficientFunds', error_log_df_path)
                print('Error: Cannot buy at price {:.2f} {} due to insufficient fund!!!'.format(price, quote_currency))
        except ccxt.InsufficientFunds:
            # not enough fund (could caused by wrong account), stop the process
            update_error_log('InsufficientFunds', error_log_df_path)
            print('Error: Cannot buy at price {:.2f} {} due to insufficient fund!!!'.format(price, quote_currency))
            sys.exit(1)


def check_circuit_breaker(bot_name, exchange, symbol, base_currency, quote_currency, last_price, grid, value, circuit_limit, idle_stage, idle_rest, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path):
    cont_flag = 1

    last_loop_price = get_last_loop_price(last_loop_path)
    transactions_df = pd.read_csv(transactions_df_path)

    if len(transactions_df) >= circuit_limit:
        side_list = transactions_df['side'][-circuit_limit:].unique()
        
        if (len(side_list) == 1) & (side_list[0] == 'buy'):
            if last_price <= last_loop_price:
                cont_flag = 0

                cancel_open_buy_orders(exchange, symbol, base_currency, quote_currency, grid, value, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path)
                update_last_loop_price(exchange, symbol, last_loop_path)
                noti_warning(bot_name, 'Circuit breaker at {} {}'.format(last_price, quote_currency))
                time.sleep(idle_rest)

    return cont_flag


def check_cut_loss(exchange, bot_name, symbol, quote_currency, last_price, grid, value, config_params_path, last_loop_path, open_orders_df_path, cash_flow_df_path, idle_stage):
    balance = exchange.fetch_balance()
    quote_currency_amount = balance[quote_currency]['free']

    open_orders_df = pd.read_csv(open_orders_df_path)
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    remain_cash_flow_accum = sum(cash_flow_df['remain_cash_flow'])
    
    if quote_currency_amount < remain_cash_flow_accum + value:
        min_sell_price = min(open_orders_df['price'])
        
        if (min_sell_price - last_price) >= (grid * 2):
            max_sell_price = max(open_orders_df['price'])
            canceled_df = open_orders_df[open_orders_df['price'] == max_sell_price]
            
            canceled_id = canceled_df['order_id'].reset_index(drop = True)[0]
            canceled_amount = canceled_df['amount'].reset_index(drop = True)[0]
            buy_price = max_sell_price - grid
            buy_value = buy_price * canceled_amount

            exchange.cancel_order(canceled_id, symbol)
            canceled_order = exchange.fetch_order(canceled_id, symbol)
            time.sleep(idle_stage)
            
            while canceled_order['status'] != 'canceled':
                time.sleep(idle_stage)
                canceled_order = exchange.fetch_order(canceled_id, symbol)

            sell_order = exchange.create_order(symbol, 'market', 'sell', canceled_amount)
            time.sleep(idle_stage)
            
            while sell_order['status'] != 'closed':
                time.sleep(idle_stage)
                sell_order = exchange.fetch_order(sell_order['id'], symbol)

            new_sell_price = sell_order['price']
            new_sell_amount = sell_order['amount']
            new_sell_value = new_sell_price * new_sell_amount
            loss = new_sell_value - buy_value

            if loss <= remain_cash_flow_accum:
                update_used_cash_flow(loss, last_loop_path)
            else:
                reduce_budget(loss, config_params_path)

            noti_warning(bot_name, 'Cut loss {} {} at {} {}'.format(loss, quote_currency, last_price, quote_currency))


def reinvest(exchange, bot_name, symbol, last_price, init_budget, budget, grid, value, fluctuation_rate, reinvest_ratio, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path):
    reinvest_flag = 0

    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transactions_df = pd.read_csv(transactions_df_path)

    try:
        last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
        last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    except IndexError:
        last_date = None
    
    cur_date = get_date()
    prev_date = cur_date - dt.timedelta(days = 1)
    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    # skip 1st date
    if (len(last_transactions_df) > 0) | (len(cash_flow_df) > 0):
        if last_date != prev_date:
            reinvest_flag = 1

            balance = get_balance(exchange, symbol, last_price)
            unrealised, _, _, _ = cal_unrealised(last_price, grid, open_orders_df)

            used_cash_flow = get_used_cash_flow(last_loop_path)
            last_sell_df = last_transactions_df[last_transactions_df['side'] == 'sell']
            profit = sum(last_sell_df['amount'] * grid)
            cash_flow = profit - used_cash_flow
            
            if reinvest_ratio == -1:
                greed_index = get_greed_index()
                reinvest_ratio = max(1 - (greed_index / 100), 0)

            reinvest_amount = cash_flow * reinvest_ratio
            remain_cash_flow = cash_flow - reinvest_amount

            deposit, withdraw = get_transfer(transfer_path)
            lower_price = last_price * (1 - fluctuation_rate)
            upper_price = last_price * (1 + fluctuation_rate)
            n_order = int((upper_price - lower_price) / grid)

            init_budget += (deposit - withdraw)
            new_budget = budget + reinvest_amount + deposit - withdraw
            new_value = new_budget / n_order

            append_cash_flow_df(prev_date, balance, unrealised, value, used_cash_flow, cash_flow, reinvest_amount, remain_cash_flow, deposit, withdraw, cash_flow_df, cash_flow_df_path)
            update_reinvest(init_budget, new_budget, new_value, config_params_path)
            reset_used_cash_flow(last_loop_path)
            reset_transfer(transfer_path)

    return reinvest_flag