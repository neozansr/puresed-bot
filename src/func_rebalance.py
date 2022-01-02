import ccxt
import pandas as pd
import time
import sys

from pandas.core.indexes import base

from func_get import get_json, get_time, get_currency, get_bid_price, get_ask_price, get_last_price, get_position, get_base_currency_value, get_cash_value, get_total_value, get_order_fee, get_available_cash_flow
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


def update_budget(transfer, config_params, config_params_path):
    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer != 0:
        budget = config_params['budget']
        budget += net_transfer
        
        config_params = get_json(config_params_path)
        config_params['budget'] = budget

        update_json(config_params, config_params_path)


def append_profit_rebalance(order, exchange, symbol, fix_value, last_loop_path, profit_df_path):
    timestamp = get_time()
    last_loop = get_json(last_loop_path)
    average_cost = last_loop['symbol'][symbol]['average_cost']
    holding_amount = last_loop['symbol'][symbol]['holding_amount']
    fee = get_order_fee(order, exchange, symbol)

    if ((order['side'] == 'buy') & (fix_value >= 0)) | ((order['side'] == 'sell') & (fix_value < 0)):
        adjusted_price = cal_adjusted_price(order, fee)
        average_cost = ((average_cost * holding_amount) + (adjusted_price * order['amount'])) / (holding_amount + order['amount'])
        holding_amount += order['amount']
    elif ((order['side'] == 'sell') & (fix_value >= 0)) | ((order['side'] == 'buy') & (fix_value < 0)):
        profit = ((order['price'] - average_cost) * order['amount']) - fee
        holding_amount -= order['amount']

        profit_df = pd.read_csv(profit_df_path)
        profit_df.loc[len(profit_df)] = [timestamp, symbol, order['price'], average_cost, order['amount'], profit]
        profit_df.to_csv(profit_df_path, index=False)

    last_loop['symbol'][symbol]['average_cost'] = average_cost
    last_loop['symbol'][symbol]['holding_amount'] = holding_amount
    last_loop['symbol'][symbol]['last_action_price'] = order['price']
    update_json(last_loop, last_loop_path)
    

def cal_min_value(symbol, grid_percent, last_loop_path):
    last_loop = get_json(last_loop_path)

    grid = last_loop['symbol'][symbol]['last_action_price'] * (grid_percent / 100)
    target_price = last_loop['symbol'][symbol]['last_action_price'] + grid
    last_action_value = last_loop['symbol'][symbol]['last_action_price'] * last_loop['symbol'][symbol]['holding_amount']
    min_value = (target_price * last_loop['symbol'][symbol]['holding_amount']) - last_action_value

    return min_value

    
def clear_orders_rebalance(exchange, bot_name, symbol, fix_value, config_system, open_orders_df_path, transactions_df_path, last_loop_path, profit_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        for order_id in open_orders_df['order_id']:
            order = exchange.fetch_order(order_id, symbol)

            while order['status'] != 'closed':
                order = exchange.fetch_order(order_id, symbol)
                time.sleep(config_system['idle_stage'])
            
            remove_order(order_id, open_orders_df_path)
            append_order(order, 'filled', transactions_df_path)
            append_profit_rebalance(order, exchange, symbol, fix_value, last_loop_path, profit_df_path)
            noti_success_order(order, bot_name, symbol)
    

def rebalance(exchange, bot_name, symbol, config_system, config_params, open_orders_df_path, transactions_df_path, last_loop_path, profit_df_path):
    rebalance_flag = 1

    base_currency, quote_currency = get_currency(symbol)
    last_price = get_last_price(exchange, symbol)
    current_value = get_base_currency_value(last_price, exchange, symbol)
    min_value = cal_min_value(symbol, config_params['grid_percent'], last_loop_path)
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
    clear_orders_rebalance(exchange, bot_name, symbol, fix_value, config_system, open_orders_df_path, transactions_df_path, last_loop_path, profit_df_path)


def update_end_date_rebalance(prev_date, exchange, bot_name, config_params, config_params_path, transfer_path, profit_df_path, cash_flow_df_path):
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
        end_cash]

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