import pandas as pd
import time
import sys

from func_get import get_json, get_time, get_currency, get_bid_price, get_ask_price, get_last_price, get_base_currency_value, get_quote_currency_value, get_available_cash_flow
from func_cal import cal_end_balance
from func_update import update_json, append_order, remove_order, append_cash_flow_df, update_transfer
from func_noti import noti_success_order, print_current_balance, print_current_value


def gen_fix_series(config_system):
    series = [config_system['idle_loop']]

    return series


def gen_hexa_series(n=18, limit_min=4):
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


def get_series_loop(config_params, config_system, last_loop_path):
    last_loop = get_json(last_loop_path)

    if config_params['series_rule'] == 'fix':
        series = gen_fix_series(config_system)
    elif config_params['series_rule'] == 'hexa':
        series = gen_hexa_series()

    order_loop = last_loop['order_loop']
    series_loop = series[order_loop]

    update_order_loop(order_loop, series, last_loop, last_loop_path)
    
    return series_loop


def update_order_loop(order_loop, series, last_loop, last_loop_path):
    order_loop += 1
    if order_loop >= len(series):
        order_loop = 0

    last_loop['order_loop'] = order_loop

    update_json(last_loop, last_loop_path)


def reset_order_loop(last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['order_loop'] = 0

    update_json(last_loop, last_loop_path)


def update_fix_value(transfer, config_params, config_params_path):
    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer != 0:
        # At leverage 1, hold value and cash with same amount.
        cash_ratio = 1 / config_params['leverage']
        fix_value = config_params['fix_value']
        fix_value += (net_transfer / (1 + cash_ratio))
        
        config_params = get_json(config_params_path)
        config_params['fix_value'] = fix_value

        update_json(config_params, config_params_path)


def append_profit_rebalance(order, config_params, last_loop_path, profit_df_path):
    timestamp = get_time()
    last_loop = get_json(last_loop_path)
    average_cost = last_loop['average_cost']
    holding_amount = last_loop['holding_amount']

    if order['side'] == 'buy':
        holding_amount += order['amount']
        average_cost = ((average_cost * holding_amount) + (order['price'] * order['amount'])) / (order['amount'] + holding_amount)
    elif order['side'] == 'sell':
        holding_amount -= order['amount']
        profit = (order['price'] - average_cost) * order['amount']

        profit_df = pd.read_csv(profit_df_path)
        profit_df.loc[len(profit_df)] = [timestamp, config_params['symbol'], order['price'], average_cost, order['amount'], profit]
        profit_df.to_csv(profit_df_path, index=False)

    last_loop['average_cost'] = average_cost
    last_loop['holding_amount'] = holding_amount
    update_json(last_loop, last_loop_path)
    

def clear_orders_rebalance(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path, last_loop_path, profit_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, config_params['symbol'])

        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, config_params['symbol'])
            time.sleep(config_system['idle_stage'])
        
        remove_order(order_id, open_orders_df_path)
        append_order(order, 'filled', transactions_df_path)
        append_profit_rebalance(order, config_params, last_loop_path, profit_df_path)
        noti_success_order(order, bot_name, config_params)


def rebalance(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path, last_loop_path, profit_df_path):
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

        # If InvalidOrder, the price is too high for current fix_value.
        order = exchange.create_order(config_params['symbol'], 'market', side, amount)
        
        append_order(order, 'amount', open_orders_df_path)

    time.sleep(config_system['idle_stage'])
    clear_orders_rebalance(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path, last_loop_path, profit_df_path)


def update_end_date_rebalance(prev_date, exchange, bot_name, config_params, config_params_path, transfer_path, profit_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    last_price = get_last_price(exchange, config_params)
    base_currency, quote_currency = get_currency(config_params)
    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    cash_flow = sum(last_profit_df['profit'])

    transfer = get_json(transfer_path)

    available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
    available_cash_flow += cash_flow
    
    end_balance = cal_end_balance(current_value, cash, transfer)

    cash_flow_list = [
        prev_date,
        config_params['fix_value'],
        end_balance,
        current_value,
        cash,
        cash_flow,
        transfer['deposit'],
        transfer['withdraw'],
        transfer['withdraw_cash_flow'],
        available_cash_flow
        ]
    
    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_fix_value(transfer, config_params, config_params_path)
    update_transfer(transfer_path)


def print_report_rebalance(exchange, config_params):
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)
    current_value = get_base_currency_value(last_price, exchange, base_currency)

    print_current_balance(last_price, exchange, config_params)
    print_current_value(current_value, exchange, quote_currency)