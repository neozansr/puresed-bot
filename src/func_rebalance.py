import ccxt
import pandas as pd
import datetime as dt
import json
import sys

from func_get import get_time, get_bid_price, get_ask_price, get_balance, get_current_value, get_last_loop, get_transfer, get_available_cash_flow
from func_update import append_order, remove_order, append_error_log, append_cash_flow_df, reset_transfer
from func_noti import noti_success_order


def gen_series(n=18, limit_min=4):
    # haxanacci
    def hexa(n) :
        if n in range(6):
            return 0
        elif n == 6:
            return 1
        else :
            return (hexa(n - 1) +
                    hexa(n - 2) +
                    hexa(n - 3) +
                    hexa(n - 4) +
                    hexa(n - 5) +
                    hexa(n - 6))
    
    series = []
    for i in range(6, n):
        series.append(hexa(i))
        
    series = [x for x in series if x >= limit_min]
    
    if len(series) == 0:
        print('No series generated, increase n size')
        sys.exit(1)
        
    return series


def get_series_loop(last_loop_path):
    last_loop = get_last_loop(last_loop_path)
    series = gen_series()

    order_loop = last_loop['order_loop']
    series_loop = series[order_loop]

    update_order_loop(order_loop, series, last_loop, last_loop_path)
    
    return series_loop


def update_order_loop(order_loop, series, last_loop, last_loop_path):
    order_loop += 1
    if order_loop >= len(series):
        order_loop = 0

    last_loop['order_loop'] = order_loop

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def reset_order_loop(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    last_loop['order_loop'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_fix_value(transfer, config_params, config_params_path):
    fix_value = config_params['fix_value']
    fix_value += ((transfer['deposit'] - transfer['withdraw']) / 2)

    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    config_params['fix_value'] = fix_value

    with open(config_params_path, 'w') as config_params_path:
        json.dump(config_params, config_params_path, indent=1)


def append_profit(sell_order, exe_amount, config_params, queue_df, profit_df_path):
    df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    buy_id = queue_df['order_id'][len(queue_df) - 1]
    sell_id = sell_order['id']
    buy_price = queue_df['price'][len(queue_df) - 1]
    sell_price = sell_order['price']
    profit = (sell_price - buy_price) * exe_amount

    df.loc[len(df)] = [timestamp, buy_id, sell_id, config_params['symbol'], exe_amount, buy_price, sell_price, profit]
    df.to_csv(profit_df_path, index=False)


def update_queue(method, sell_order, amount_key, config_params, queue_df_path, profit_df_path):
    sell_amount = sell_order[amount_key]

    while sell_amount > 0:
        queue_df = pd.read_csv(queue_df_path)
        
        if method == 'fifo':
            order_index = 0
        elif method == 'lifo':
            order_index = len(queue_df) - 1
    
        sell_queue = queue_df['amount'][order_index]
        exe_amount = min(sell_amount, sell_queue)
        remaining_queue = sell_queue - exe_amount

        append_profit(sell_order, exe_amount, config_params, queue_df, profit_df_path)
        
        if remaining_queue == 0:
            queue_df = queue_df.drop([order_index])
        else:
            queue_df.loc[order_index, 'amount'] = remaining_queue

        queue_df.to_csv(queue_df_path, index=False)
        sell_amount -= exe_amount


def cancel_open_orders_rebalance(order_id, exchange, config_params, open_orders_df_path, error_log_df_path):
    try:
        exchange.cancel_order(order_id, config_params['symbol'])
        remove_order(order_id, open_orders_df_path)
        print(f'Cancel order {order_id}')
        cont_flag = 1
    except ccxt.OrderNotFound:
        # no order in the system (could casued by the order is queued), skip for the next loop
        append_error_log('OrderNotFound', error_log_df_path)
        print(f'Error: Cannot cancel order {order_id}, wait for the next loop')
        cont_flag = 0
    except ccxt.InvalidOrder:
        # the order is closed by system (could caused by post_only param for buy orders)
        remove_order(open_orders_df_path, order_id)
        cont_flag = 1

    return cont_flag


def clear_queue(method, order_id, exchange, config_params, open_orders_df_path, error_log_df_path):
    if method == 'lifo':
        cont_flag = cancel_open_orders_rebalance(order_id, exchange, config_params, open_orders_df_path, error_log_df_path)
    elif method == 'fifo':
        # fifo method have to be closed wihtin the loop 
        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, config_params['symbol'])
        
        cont_flag = 1

    return cont_flag


def clear_orders_rebalance(method, exchange, bot_name, base_currency, quote_currency, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path):
    cont_flag = 1
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, config_params['symbol'])

        if order['filled'] > 0:
            if order['status'] != 'closed':
                cont_flag = clear_queue(method, order_id, exchange, config_params, open_orders_df_path, error_log_df_path)
        
            if order['side'] == 'buy':
                append_order('filled', order, config_params, queue_df_path)
            elif order['side'] == 'sell':
                update_queue(method, order, 'filled', config_params, queue_df_path, profit_df_path)

            remove_order(order_id, open_orders_df_path)
            append_order('filled', order, config_params, transactions_df_path)
            noti_success_order(order, bot_name, base_currency, quote_currency)
        
        else:
            cont_flag = clear_queue(method, order_id, exchange, config_params, open_orders_df_path, error_log_df_path)

    return cont_flag


def rebalance(current_value, exchange, base_currency, quote_currency, config_params, open_orders_df_path, error_log_df_path):
    rebalance_flag = 1

    if current_value < config_params['fix_value'] - config_params['min_value']:
        side = 'buy'
        diff_value = config_params['fix_value'] - current_value
        price = get_bid_price(exchange, config_params)
    elif current_value > config_params['fix_value'] + config_params['min_value']:
        side = 'sell'
        diff_value = current_value - config_params['fix_value']
        price = get_ask_price(exchange, config_params)
    else:
        rebalance_flag = 0
        print('No action')
        
    if rebalance_flag == 1:
        amount = diff_value / price
        try:
            order = exchange.create_order(config_params['symbol'], 'market', side, amount)
            append_order('amount', order, config_params, open_orders_df_path)
            print(f'Open {side} {amount:.3f} {base_currency} at {price:.2f} {quote_currency}')
        except ccxt.InsufficientFunds: 
            # not enough fund (could caused by wrong account), stop the process
            append_error_log('InsufficientFunds', error_log_df_path)
            print(f'Error: Cannot {side} at price {price:.2f} {quote_currency} due to insufficient fund!!!')
            sys.exit(1)


def update_withdraw_flag(last_loop_path, enable):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    # enable flag when withdraw detected
    # disable flag after sell assets
    if enable == True:
        last_loop['withdraw_flag'] = 1
    else:
        last_loop['withdraw_flag'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_budget_rebalance(last_price, prev_date, exchange, bot_name, quote_currency, config_params, config_params_path, last_loop_path, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transactions_df = pd.read_csv(transactions_df_path)

    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    if (len(last_transactions_df) > 0) | (len(cash_flow_df) > 0):
        balance, cash = get_balance(last_price, exchange, config_params)

        profit_df = pd.read_csv(profit_df_path)
        last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
        cash_flow = sum(last_profit_df['profit'])
        
        transfer = get_transfer(transfer_path)

        available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
        available_cash_flow += cash_flow
        
        cash_flow_list = [prev_date, balance, cash, config_params['fix_value'], cash_flow, transfer['withdraw_cash_flow'], available_cash_flow, transfer['deposit'], transfer['withdraw']]
        append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)

        net_transfer = transfer['deposit'] - transfer['withdraw']

        if net_transfer != 0:
            update_fix_value(transfer, config_params, config_params_path)
            
            if net_transfer < 0:
                update_withdraw_flag(last_loop_path, True)

        reset_transfer(transfer_path)