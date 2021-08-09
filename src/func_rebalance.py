import ccxt
import pandas as pd
import time
import json
import sys

from func_get import get_json, get_time, get_currency, get_bid_price, get_ask_price, get_last_price, get_base_currency_value, get_quote_currency_value, get_available_cash_flow
from func_update import append_order, remove_order, append_error_log, append_cash_flow_df, reset_transfer
from func_noti import noti_success_order, print_current_balance, print_current_value


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
        print("No series generated, increase n size")
        sys.exit(1)
        
    return series


def get_series_loop(last_loop_path):
    last_loop = get_json(last_loop_path)
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
    last_loop = get_json(last_loop_path)
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


def append_profit_rebalance(sell_order, exe_amount, config_params, queue_df, profit_df_path):
    df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    buy_id = queue_df['order_id'][len(queue_df) - 1]
    sell_id = sell_order['id']
    buy_price = queue_df['price'][len(queue_df) - 1]
    sell_price = sell_order['price']
    profit = (sell_price - buy_price) * exe_amount

    df.loc[len(df)] = [timestamp, buy_id, sell_id, config_params['symbol'], exe_amount, buy_price, sell_price, profit]
    df.to_csv(profit_df_path, index=False)


def update_queue(method, amount_key, sell_order, config_params, queue_df_path, profit_df_path):
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

        append_profit_rebalance(sell_order, exe_amount, config_params, queue_df, profit_df_path)
        
        if remaining_queue == 0:
            queue_df = queue_df.drop([order_index])
        else:
            queue_df.loc[order_index, 'amount'] = remaining_queue

        queue_df.to_csv(queue_df_path, index=False)
        sell_amount -= exe_amount
    

def clear_orders_rebalance(method, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, config_params['symbol'])

        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, config_params['symbol'])
            time.sleep(config_system['idle_stage'])
    
        if order['side'] == 'buy':
            append_order(order, 'filled', queue_df_path)
        elif order['side'] == 'sell':
            update_queue(method, 'filled', order, config_params, queue_df_path, profit_df_path)

        remove_order(order_id, open_orders_df_path)
        append_order(order, 'filled', transactions_df_path)
        noti_success_order(order, bot_name, config_params)


def rebalance(method, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path, error_log_df_path):
    rebalance_flag = 1

    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)
    current_value = get_base_currency_value(last_price, exchange, base_currency)
    
    print(f"Last price: {last_price:.2f} {quote_currency}")
    print_current_value(current_value, exchange, quote_currency)

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
        print("No action")
        
    if rebalance_flag == 1:
        amount = diff_value / price
        try:
            order = exchange.create_order(config_params['symbol'], 'market', side, amount)
            append_order(order, 'amount', open_orders_df_path)
        except ccxt.InsufficientFunds: 
            # not enough fund (could caused by wrong account), stop the process
            append_error_log('InsufficientFunds', error_log_df_path)
            print(f"Error: Cannot {side} at price {last_price:.2f} {quote_currency} due to insufficient fund!!!")
            sys.exit(1)

    time.sleep(config_system['idle_stage'])
    clear_orders_rebalance(method, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path)


def update_withdraw_flag(last_loop_path, enable):
    last_loop = get_json(last_loop_path)

    # enable flag when withdraw detected
    # disable flag after sell assets
    if enable == True:
        last_loop['withdraw_flag'] = 1
    else:
        last_loop['withdraw_flag'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_budget_rebalance(prev_date, exchange, bot_name, config_params, config_params_path, transfer_path, profit_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)

    withdraw_flag = 0

    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)
    balance_value = current_value + cash

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    cash_flow = sum(last_profit_df['profit'])
    
    transfer = get_json(transfer_path)

    available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
    available_cash_flow += cash_flow
    
    cash_flow_list = [prev_date, balance_value, cash, config_params['fix_value'], cash_flow, transfer['withdraw_cash_flow'], available_cash_flow, transfer['deposit'], transfer['withdraw']]
    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)

    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer != 0:
        update_fix_value(transfer, config_params, config_params_path)
        
        if net_transfer < 0:
            withdraw_flag = 1

    reset_transfer(transfer_path)

    return withdraw_flag


def print_report_rebalance(exchange, config_params):
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)
    current_value = get_base_currency_value(last_price, exchange, base_currency)

    print_current_balance(last_price, exchange, config_params)
    print_current_value(current_value, exchange, quote_currency)