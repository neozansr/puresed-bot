import pandas as pd
import time
import sys

from func_get import get_json, get_time, get_currency, get_bid_price, get_ask_price, get_last_price, get_base_currency_amount, get_base_currency_value, get_cash_value, get_total_value, get_order_fee, get_available_cash_flow
from func_cal import round_amount, cal_adjusted_price, cal_end_balance, cal_end_cash
from func_update import update_json, append_order, remove_order, append_cash_flow_df, update_transfer
from func_noti import noti_success_order


def gen_fix_sequence(config_system):
    sequence = [config_system['idle_loop']]

    return sequence


def gen_hexa_sequence(n=18, limit_min=4):
    def hexa(n) :
        if n in range(6):
            return 0
        elif n == 6:
            return 1
        else:
            return (hexa(n - 1) +
                    hexa(n - 2) +
                    hexa(n - 3) +
                    hexa(n - 4) +
                    hexa(n - 5) +
                    hexa(n - 6))
    
    sequence = []
    for i in range(6, n):
        sequence.append(hexa(i))
        
    sequence = [x for x in sequence if x >= limit_min]
    
    if len(sequence) == 0:
        print("No sequence generated, increase n size!!!")
        sys.exit(1)
        
    return sequence


def get_sequence_loop(config_params, config_system, last_loop_path):
    last_loop = get_json(last_loop_path)

    if config_params['sequence_rule'] == 'fix':
        sequence = gen_fix_sequence(config_system)
    elif config_params['sequence_rule'] == 'hexa':
        sequence = gen_hexa_sequence()

    order_loop = last_loop['order_loop']
    sequence_loop = sequence[order_loop]

    update_order_loop(order_loop, sequence, last_loop, last_loop_path)
    
    return sequence_loop


def update_order_loop(order_loop, sequence, last_loop, last_loop_path):
    order_loop += 1
    if order_loop >= len(sequence):
        order_loop = 0

    last_loop['order_loop'] = order_loop

    update_json(last_loop, last_loop_path)


def reset_order_loop(last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['order_loop'] = 0

    update_json(last_loop, last_loop_path)


def update_budget(transfer, config_params, config_params_path, last_loop_path):
    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer != 0:
        budget = config_params['budget']
        budget += net_transfer
        
        config_params = get_json(config_params_path)
        config_params['budget'] = budget

        last_loop = get_json(last_loop_path)
        last_loop['transfer_flag'] = 1

        update_json(config_params, config_params_path)
        update_json(last_loop, last_loop_path)


def append_profit_rebalance(sell_order, exchange, exe_amount, symbol, queue_df, profit_df_path):
    profit_df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    buy_id = queue_df['order_id'][len(queue_df) - 1]
    sell_id = sell_order['id']
    buy_price = queue_df['price'][len(queue_df) - 1]
    sell_price = sell_order['price']

    fee = get_order_fee(sell_order, exchange, symbol)
    adjusted_price = cal_adjusted_price(sell_order, fee, side='sell')
    profit = (adjusted_price - buy_price) * exe_amount

    profit_df.loc[len(profit_df)] = [timestamp, buy_id, sell_id, symbol, exe_amount, buy_price, sell_price, profit]
    profit_df.to_csv(profit_df_path, index=False)


def update_hold(buy_order, exchange, symbol, queue_df_path):
    queue_df = pd.read_csv(queue_df_path.format(symbol))

    timestamp = get_time()
    hold_amount = queue_df.loc[0, 'amount']
    hold_price = queue_df.loc[0, 'price']
    
    order_amount = buy_order['filled']
    
    fee = get_order_fee(buy_order, exchange, symbol)
    adjusted_price = cal_adjusted_price(buy_order, fee, side='buy')

    new_hold_amount = hold_amount + order_amount
    new_hold_price = ((hold_amount * hold_price) + (order_amount * adjusted_price)) / new_hold_amount

    queue_df.loc[0, 'timestamp'] = timestamp
    queue_df.loc[0, 'amount'] = new_hold_amount
    queue_df.loc[0, 'price'] = new_hold_price
    queue_df.to_csv(queue_df_path, index=False)


def update_queue(sell_order, exchange, method, amount_key, symbol, queue_df_path, profit_df_path):
    sell_amount = sell_order[amount_key]

    while sell_amount > 0:
        queue_df = pd.read_csv(queue_df_path.format(symbol))
        
        if method == 'fifo':
            order_index = 0
        elif method == 'lifo':
            order_index = len(queue_df) - 1
    
        sell_queue = queue_df['amount'][order_index]
        exe_amount = min(sell_amount, sell_queue)
        remaining_queue = sell_queue - exe_amount

        if method == 'lifo':
            append_profit_rebalance(sell_order, exchange, exe_amount, symbol, queue_df, profit_df_path)
        
        if remaining_queue == 0:
            queue_df = queue_df.drop([order_index]).reset_index(drop=True)
        else:
            queue_df.loc[order_index, 'amount'] = remaining_queue

        queue_df.to_csv(queue_df_path, index=False)
        sell_amount -= exe_amount
    

def cal_min_value(exchange, symbol, grid_percent, last_loop_path):
    last_loop = get_json(last_loop_path)

    amount = get_base_currency_amount(exchange, symbol)
    grid = last_loop['symbol'][symbol]['last_action_price'] * (grid_percent / 100)
    min_value = grid * amount

    return min_value

    
def clear_orders_rebalance(method, exchange, bot_name, symbol, config_system, last_loop_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, symbol)

        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, symbol)
            time.sleep(config_system['idle_stage'])
    
        if order['side'] == 'buy':
            if method == 'lifo':
                append_order(order, 'filled', queue_df_path)
            elif method == 'fifo':
                update_hold(order, exchange, symbol, queue_df_path)
        
        elif order['side'] == 'sell':
            update_queue(order, exchange, method, 'filled', symbol, queue_df_path, profit_df_path)

        last_loop = get_json(last_loop_path)
        last_loop['symbol'][symbol]['last_action_price'] = order['price']

        remove_order(order_id, open_orders_df_path)
        append_order(order, 'filled', transactions_df_path)
        update_json(last_loop, last_loop_path)
        noti_success_order(order, bot_name, symbol)
    

def rebalance(exchange, bot_name, symbol, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path):
    rebalance_flag = 1

    base_currency, quote_currency = get_currency(symbol)
    last_price = get_last_price(exchange, symbol)
    current_value = get_base_currency_value(last_price, exchange, symbol)
    min_value = cal_min_value(exchange, symbol, config_params['grid_percent'], last_loop_path)
    fix_value = config_params['budget'] * config_params['symbol'][symbol]

    print(f"Last price: {last_price} {quote_currency}")
    print(f"Fix value: {fix_value} USD")
    print(f"Min value: {min_value} USD")
    print(f"Current value: {current_value} USD")

    if current_value < fix_value - min_value:
        side = 'buy'
        diff_value = fix_value - current_value
        price = get_bid_price(exchange, symbol)
    elif current_value > fix_value + min_value:
        side = 'sell'
        diff_value = current_value - fix_value
        price = get_ask_price(exchange, symbol)
    else:
        rebalance_flag = 0
        print("No action")
        
    if rebalance_flag == 1:
        amount = diff_value / price
        rounded_amount = round_amount(amount, exchange, symbol, type='down')

        if rounded_amount > 0:
            print(f"Diff value: {diff_value} USD")
            order = exchange.create_order(symbol, 'market', side, rounded_amount)
            append_order(order, 'amount', open_orders_df_path)
        else:
            print(f"Cannot {side} {diff_value} value, {amount} {base_currency} is too small amount to place order!!!")

    time.sleep(config_system['idle_stage'])

    last_loop = get_json(last_loop_path)

    if last_loop['transfer_flag'] == 1:
        method = 'fifo'
        last_loop['transfer_flag'] = 0
        update_json(last_loop, last_loop_path)
    else:
        method = 'lifo'

    clear_orders_rebalance(method, exchange, bot_name, symbol, config_system, last_loop_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path)


def update_end_date_rebalance(prev_date, exchange, bot_name, config_params, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transfer = get_json(transfer_path)
    
    total_value, value_dict = get_total_value(exchange, config_params)
    cash = get_cash_value(exchange)

    end_balance = cal_end_balance(total_value, cash, transfer)
    end_cash = cal_end_cash(cash, transfer)

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    cash_flow = sum(last_profit_df['profit'])

    available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
    available_cash_flow += cash_flow

    pre_cash_flow_list = [
        prev_date,
        config_params['budget'],
        end_balance,
        end_cash
        ]

    value_list = []
    for symbol in value_dict.keys():
        value_list.append(value_dict[symbol]['current_value'])

    post_cash_flow_list = [
        cash_flow,
        transfer['deposit'],
        transfer['withdraw'],
        transfer['withdraw_cash_flow'],
        available_cash_flow
        ]

    cash_flow_list = pre_cash_flow_list + value_list  + post_cash_flow_list
    
    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_budget(transfer, config_params, config_params_path)
    update_transfer(config_params['taker_fee'], transfer_path)