import ccxt
import pandas as pd
import datetime as dt
import time

from func_get import get_time, get_bid_price, get_ask_price, get_last_loop_price, get_transfer, get_available_cash_flow
from func_get import update_last_loop_price, update_loss, reduce_budget
from func_cal import round_down_amount, cal_final_amount, cal_sell_price, cal_buy_price_list
from func_noti import noti_success_order, noti_warning


def append_order(df_path, order, symbol, amount_key):
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


def remove_order(df_path, order_id):
    df = pd.read_csv(df_path)

    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop = True)
    df.to_csv(df_path, index = False)


def append_error_log(error_log, error_log_df_path):
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
        final_amount = round_down_amount(base_currency_amount, decimal)
        sell_order = exchange.create_order(symbol, 'limit', 'sell', final_amount, sell_price)
        append_error_log('InsufficientFunds', error_log_df_path)
    except ccxt.InvalidOrder:
        # filled small value than minimum order, ignore
        append_error_log('InvalidOrder', error_log_df_path)
    
    print('Open sell {:.3f} {} at {} {}'.format(final_amount, base_currency, sell_price, quote_currency))
    return sell_order


def check_orders_status(exchange, bot_name, side, symbol, base_currency, quote_currency, grid, decimal, idle_stage, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()

    if side == 'sell':
        # buy orders: FIFO
        # sell orders: LIFO
        open_orders_list.reverse()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, symbol)
        if order['status'] == 'closed':
            noti_success_order(bot_name, order, symbol, base_currency, quote_currency)

            if side == 'buy':
                sell_order = open_sell_order(exchange, order, symbol, base_currency, quote_currency, grid, decimal, idle_stage, error_log_df_path)
                append_order(open_orders_df_path, sell_order, symbol, amount_key = 'amount')

            remove_order(open_orders_df_path, order_id)
            append_order(transactions_df_path, order, symbol, amount_key = 'filled')

        elif order['status'] == 'canceled':
            # canceld by param PostOnly
            remove_order(open_orders_df_path, order_id)


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
                    append_order(transactions_df_path, order, symbol, amount_key = 'filled')
                    sell_order = open_sell_order(exchange, order, symbol, base_currency, quote_currency, grid, decimal, idle_stage, error_log_df_path)
                    append_order(open_orders_df_path, sell_order, symbol, amount_key = 'amount')
                
                remove_order(open_orders_df_path, order_id)
            except ccxt.OrderNotFound:
                # no order in the system (could casued by the order is queued), skip for the next loop
                append_error_log('OrderNotFound', error_log_df_path)
                print('Error: Cannot cancel order {} due to unavailable order!!!'.format(order_id))
            except ccxt.InvalidOrder:
                # the order is closed by system (could caused by post_only param for buy orders)
                remove_order(open_orders_df_path, order_id)


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
        floor_amount = round_down_amount(amount, decimal)
        
        balance = exchange.fetch_balance()
        quote_currency_amount = balance[quote_currency]['free']

        if quote_currency_amount >= remain_cash_flow_accum + value:
            buy_order = exchange.create_order(symbol, 'limit', 'buy', floor_amount, price, params = {'postOnly':True})
            append_order(open_orders_df_path, buy_order, symbol, amount_key = 'amount')
            print('Open buy {:.3f} {} at {} {}'.format(floor_amount, base_currency, price, quote_currency))
        else:
            # actual buget less than cal_budget (could caused by open_orders match during loop)
            print('Error: Cannot buy at price {} {} due to insufficient fund!!!'.format(price, quote_currency))
            break


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


def check_cut_loss(exchange, bot_name, symbol, quote_currency, last_price, grid, value, config_params_path, last_loop_path, transfer_path, open_orders_df_path, cash_flow_df_path, idle_stage):
    balance = exchange.fetch_balance()
    quote_currency_amount = balance[quote_currency]['free']

    open_orders_df = pd.read_csv(open_orders_df_path)
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    min_sell_price = min(open_orders_df['price'])    
    if (min_sell_price - last_price) >= (grid * 2):
        # double check budget, in case of last_price shift much higher
        _, _, withdraw_cash_flow = get_transfer(transfer_path)
        available_cash_flow = get_available_cash_flow(withdraw_cash_flow, cash_flow_df)

        while quote_currency_amount < available_cash_flow + value:
            cut_loss(exchange, bot_name, symbol, quote_currency, last_price, grid, config_params_path, last_loop_path, open_orders_df, idle_stage)
            time.sleep(idle_stage)
            

def cut_loss(exchange, bot_name, symbol, quote_currency, last_price, grid, config_params_path, last_loop_path, open_orders_df, idle_stage):
    max_sell_price = max(open_orders_df['price'])
    canceled_df = open_orders_df[open_orders_df['price'] == max_sell_price]

    canceled_id = canceled_df['order_id'].reset_index(drop = True)[0]
    buy_amount = canceled_df['amount'].reset_index(drop = True)[0]
    buy_price = max_sell_price - grid
    buy_value = buy_price * buy_amount

    exchange.cancel_order(canceled_id, symbol)
    time.sleep(idle_stage)
    canceled_order = exchange.fetch_order(canceled_id, symbol)

    # cancel orders will be removed from db on the next loop by check_orders_status
    while canceled_order['status'] != 'canceled':
        time.sleep(idle_stage)
        canceled_order = exchange.fetch_order(canceled_id, symbol)

    sell_order = exchange.create_order(symbol, 'market', 'sell', buy_amount)
    time.sleep(idle_stage)

    while sell_order['status'] != 'closed':
        time.sleep(idle_stage)
        sell_order = exchange.fetch_order(sell_order['id'], symbol)

    new_sell_price = sell_order['price']
    new_sell_amount = sell_order['amount']
    new_sell_value = new_sell_price * new_sell_amount
    loss = new_sell_value - buy_value
    
    update_loss(loss, last_loop_path)
    reduce_budget(loss, config_params_path)
    noti_warning(bot_name, 'Cut loss {:.2f} {} at {} {}'.format(loss, quote_currency, last_price, quote_currency))