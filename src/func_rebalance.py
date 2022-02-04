import pandas as pd
from dateutil.relativedelta import relativedelta
import sys

from func_get import get_json, get_time, get_currency, get_bid_price, get_ask_price, get_last_price, get_base_currency_amount, get_base_currency_value, get_cash_value, get_total_value, get_order_fee, get_available_cash_flow, get_funding_payment
from func_cal import round_amount, cal_adjusted_price, cal_end_balance, cal_end_cash
from func_update import update_json, append_csv, append_order, remove_order, update_transfer
from func_noti import noti_success_order


def gen_fix_sequence(idel_sequence=10):
    sequence = [idel_sequence]

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


def update_order_loop(order_loop, sequence, last_loop, last_loop_path):
    order_loop += 1
    if order_loop >= len(sequence):
        order_loop = 0

    last_loop['order_loop'] = order_loop

    update_json(last_loop, last_loop_path)


def update_sequence_loop(config_params, last_loop_path):
    last_loop = get_json(last_loop_path)

    if config_params['sequence_rule'] == 'fix':
        sequence = gen_fix_sequence()
    elif config_params['sequence_rule'] == 'hexa':
        sequence = gen_hexa_sequence()

    order_loop = last_loop['order_loop']
    sequence_loop = sequence[order_loop]
    
    update_order_loop(order_loop, sequence, last_loop, last_loop_path)

    timestamp = get_time()
    last_loop['last_rebalance_timestamp'] = timestamp
    last_loop['next_rebalance_timestamp'] = timestamp + relativedelta(seconds=sequence_loop)

    update_json(last_loop, last_loop_path)


def reset_order_loop(last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['order_loop'] = 0
    last_loop['last_rebalance_timestamp'] = 0
    last_loop['next_rebalance_timestamp'] = 0

    update_json(last_loop, last_loop_path)


def get_rebalance_flag(last_loop_path):
    last_loop = get_json(last_loop_path)
    timestamp = get_time()

    if last_loop['next_rebalance_timestamp'] == 0:
        # First loop
        rebalance_flag = True
    elif timestamp >= last_loop['next_rebalance_timestamp']:
        rebalance_flag = True
    else:
        rebalance_flag = False

    return rebalance_flag


def update_budget(transfer, config_params, config_params_path, last_loop_path):
    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer != 0:
        config_params['budget'] += net_transfer

        last_loop = get_json(last_loop_path)
        last_loop['transfer_flag'] = 1

        update_json(config_params, config_params_path)
        update_json(last_loop, last_loop_path)


def append_profit_rebalance(sell_order, exchange, exe_amount, symbol, queue_df, profit_df_path):
    timestamp = get_time()
    profit_df = pd.read_csv(profit_df_path)

    buy_id = queue_df['order_id'][len(queue_df) - 1]
    sell_id = sell_order['id']
    buy_price = queue_df['price'][len(queue_df) - 1]

    # Sell order fee currency is always USD
    fee, _ = get_order_fee(sell_order, exchange, symbol)
    sell_price = cal_adjusted_price(sell_order, fee, side='sell')
    profit = (sell_price - buy_price) * exe_amount

    profit_df.loc[len(profit_df)] = [timestamp, buy_id, sell_id, symbol, exe_amount, buy_price, sell_price, profit]
    profit_df.to_csv(profit_df_path, index=False)


def cal_average_price(hold_amount, hold_price, added_amount, add_price):
    new_hold_amount = hold_amount + added_amount
    average_price = ((hold_amount * hold_price) + (added_amount * add_price)) / new_hold_amount

    return average_price, new_hold_amount


def update_average_cost(added_amount, add_price, exchange, symbol, last_loop_path):
    last_loop = get_json(last_loop_path)

    total_amount = get_base_currency_amount(exchange, symbol)
    hold_amount = total_amount - added_amount
    hold_price = last_loop['symbol'][symbol]['average_cost']

    average_price, _ = cal_average_price(hold_amount, hold_price, added_amount, add_price)
    last_loop['symbol'][symbol]['average_cost'] = average_price
    
    update_json(last_loop, last_loop_path)


def update_hold_cost(added_amount, add_price, timestamp, queue_df):
    hold_amount = queue_df.loc[0, 'amount']
    hold_price = queue_df.loc[0, 'price']

    average_price, new_hold_amount = cal_average_price(hold_amount, hold_price, added_amount, add_price)

    queue_df.loc[0, 'timestamp'] = timestamp
    queue_df.loc[0, 'amount'] = new_hold_amount
    queue_df.loc[0, 'price'] = average_price

    return queue_df


def update_hold(buy_order, exchange, symbol, last_loop_path, queue_df_path):
    timestamp = get_time()
    queue_df = pd.read_csv(queue_df_path)

    base_currency, quote_currency = get_currency(buy_order['symbol'])
    fee, fee_currency = get_order_fee(buy_order, exchange, symbol)

    if fee_currency == quote_currency:
        buy_amount = buy_order['filled']
        buy_price = cal_adjusted_price(buy_order, fee, side='buy')
    elif fee_currency == base_currency:
        buy_amount = buy_order['filled'] - fee
        buy_price = buy_order['price']

    update_average_cost(buy_amount, buy_price, exchange, symbol, last_loop_path)
    queue_df = update_hold_cost(buy_amount, buy_price, timestamp, queue_df)
    queue_df.to_csv(queue_df_path, index=False)


def append_queue(buy_order, exchange, last_loop_path, queue_df_path):
    timestamp = get_time()
    queue_df = pd.read_csv(queue_df_path)

    base_currency, quote_currency = get_currency(buy_order['symbol'])
    fee, fee_currency = get_order_fee(buy_order, exchange, buy_order['symbol'])

    if fee_currency == quote_currency:
        added_queue = buy_order['filled']
        buy_price = cal_adjusted_price(buy_order, fee, side='buy')
    elif fee_currency == base_currency:
        buy_amount = buy_order['filled'] - fee
        added_queue = exchange.amount_to_precision(buy_order['symbol'], buy_amount)
        buy_price = buy_order['price']

        added_hold_amount = buy_amount - added_queue
        queue_df = update_hold_cost(added_hold_amount, buy_price, timestamp, queue_df)

    update_average_cost(buy_amount, buy_price, exchange, buy_order['symbol'], last_loop_path)
    queue_df.loc[len(queue_df)] = [timestamp, buy_order['id'], added_queue, buy_price]
    queue_df.to_csv(queue_df_path, index=False)


def update_queue(sell_order, exchange, method, amount_key, symbol, queue_df_path, profit_df_path):
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

        if method == 'lifo':
            append_profit_rebalance(sell_order, exchange, exe_amount, symbol, queue_df, profit_df_path)
        
        if remaining_queue == 0:
            queue_df = queue_df.drop([order_index]).reset_index(drop=True)
        else:
            queue_df.loc[order_index, 'amount'] = remaining_queue

        queue_df.to_csv(queue_df_path, index=False)
        sell_amount -= exe_amount


def resend_order(order, exchange, symbol, open_orders_df_path):
    if order['side'] == 'buy':
        price = get_bid_price(exchange, symbol)
    elif order['side'] == 'sell':
        price = get_ask_price(exchange, symbol)
    
    order = exchange.create_order(symbol, 'limit', order['side'], order['remaining'], price)
    append_order(order, 'amount', open_orders_df_path)


def clear_orders_rebalance(exchange, bot_name, last_loop_path, open_orders_df_path, transactions_df_path, queue_df_path, profit_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    last_loop = get_json(last_loop_path)

    if last_loop['transfer_flag'] == 1:
        method = 'fifo'
        last_loop['transfer_flag'] = 0
        update_json(last_loop, last_loop_path)
    else:
        method = 'lifo'

    for order_id in open_orders_df['order_id'].unique():
        symbol = open_orders_df.loc[open_orders_df['order_id'] == order_id, 'symbol'].item()
        base_currency, _ = get_currency(symbol)
        order = exchange.fetch_order(order_id, symbol)

        if order['status'] != 'closed':
            exchange.cancel_order(order_id)

            if order['remaining'] > 0:
                resend_order(order, exchange, symbol, open_orders_df_path)

        if (order['filled'] > 0) & (order['side'] == 'buy') & (method == 'lifo'):
            append_queue(order, exchange, last_loop_path, queue_df_path.format(base_currency))
        elif (order['filled'] > 0) & (order['side'] == 'buy') & (method == 'fifo'):
            update_hold(order, exchange, symbol, last_loop_path, queue_df_path.format(base_currency))
        elif (order['filled'] > 0) & (order['side'] == 'sell'):
            update_queue(order, exchange, method, 'filled', symbol, queue_df_path.format(base_currency), profit_df_path)

        last_loop = get_json(last_loop_path)
        last_loop['symbol'][symbol]['last_action_price'] = order['price']

        remove_order(order_id, open_orders_df_path)
        append_order(order, 'filled', transactions_df_path)
        update_json(last_loop, last_loop_path)
        noti_success_order(order, bot_name, symbol)


def cal_min_value(exchange, symbol, grid_percent, last_loop_path):
    last_loop = get_json(last_loop_path)

    amount = get_base_currency_amount(exchange, symbol)
    grid = last_loop['symbol'][symbol]['last_action_price'] * (grid_percent / 100)
    min_value = grid * amount

    return min_value


def rebalance(exchange, symbol, config_params, last_loop_path, open_orders_df_path):
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
            order = exchange.create_order(symbol, 'limit', side, rounded_amount, price)
            append_order(order, 'amount', open_orders_df_path)
        else:
            print(f"Cannot {side} {diff_value} value, {amount} {base_currency} is too small amount to place order!!!")


def update_end_date_rebalance(prev_date, exchange, config_params, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path):
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transfer = get_json(transfer_path)
    
    total_value, value_dict = get_total_value(exchange, config_params)
    cash = get_cash_value(exchange)

    end_balance = cal_end_balance(total_value, cash, transfer)
    end_cash = cal_end_cash(cash, transfer)

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    cash_flow = sum(last_profit_df['profit'])
    funding_payment, _ = get_funding_payment(exchange, range='end_date')
    net_cash_flow = cash_flow - funding_payment

    available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
    available_cash_flow += net_cash_flow

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
        funding_payment,
        net_cash_flow,
        transfer['deposit'],
        transfer['withdraw'],
        transfer['withdraw_cash_flow'],
        available_cash_flow
        ]

    cash_flow_list = pre_cash_flow_list + value_list  + post_cash_flow_list
    
    append_csv(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_budget(transfer, config_params, config_params_path, last_loop_path)
    update_transfer(config_params['taker_fee'], transfer_path)