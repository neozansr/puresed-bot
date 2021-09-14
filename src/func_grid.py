import ccxt
import numpy as np
import pandas as pd
import time

from func_get import get_json, get_currency, get_bid_price, get_ask_price, get_last_price, get_base_currency_free, get_quote_currency_free, get_base_currency_value, get_quote_currency_value, get_greed_index, get_available_cash_flow, get_available_yield
from func_cal import round_down_amount, cal_final_amount, cal_unrealised
from func_update import update_json, append_order, remove_order, append_error_log, append_cash_flow_df, update_last_loop_price, reset_transfer
from func_noti import noti_success_order, noti_warning, print_current_balance, print_hold_assets, print_pending_order


def cal_sell_price(order, ask_price, config_params):
    sell_price = max(order['price'] + config_params['grid'], ask_price)

    return sell_price


def cal_budget(config_params, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    sell_price_list = open_sell_orders_df['price'].to_list()
    sell_amount_list = open_sell_orders_df['amount'].to_list()
    sell_value_list = [(i - config_params['grid']) * j for i, j in zip(sell_price_list, sell_amount_list)]
    used_budget = sum(sell_value_list)
    
    # For cal_new_orders.
    remain_budget = config_params['budget'] - used_budget
    buy_value_list = open_buy_orders_df['value'].to_list()
    pending_buy_value = sum(buy_value_list)
    
    # For cal_append_orders.
    free_budget = remain_budget - pending_buy_value

    return remain_budget, free_budget


def cal_new_orders(start_price, remain_budget, config_params):
    buy_price = start_price
    remain_n_order = int(remain_budget / np.ceil(config_params['value']))

    buy_price_list = []
    
    for _ in range(min(remain_n_order, config_params['circuit_limit'])):
        buy_price_list.append(buy_price)
        buy_price -= config_params['grid']

    return buy_price_list


def cal_append_orders(min_open_buy_price, free_budget, config_params, open_buy_orders_df):
    buy_price_list = []

    free_slot_order = config_params['circuit_limit'] - len(open_buy_orders_df)
    free_n_order = int(free_budget / np.ceil(config_params['value']))
    buy_price = min_open_buy_price - config_params['grid']

    for _ in range(min(free_n_order, free_slot_order)):
        buy_price_list.append(buy_price)
        buy_price -= config_params['grid']

    return buy_price_list


def cal_buy_price_list(remain_budget, free_budget, bid_price, config_params, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    
    max_open_buy_price = max(open_buy_orders_df['price'], default=0)
    min_open_sell_price = min(open_sell_orders_df['price'], default=np.inf)

    if min(bid_price, min_open_sell_price - config_params['grid']) - max_open_buy_price > config_params['grid']:    
        if len(open_sell_orders_df) == 0:
            start_price = bid_price - (config_params['grid'] * config_params['start_safety'])
        else:
            # Use grid * 2 to prevent dupplicate order.
            start_price = min(bid_price, min_open_sell_price - (config_params['grid'] * 2))

        buy_price_list = cal_new_orders(start_price, remain_budget, config_params)
            
        cancel_flag = 1
    else:
        min_open_buy_price = min(open_buy_orders_df['price'])
        buy_price_list = cal_append_orders(min_open_buy_price, free_budget, config_params, open_buy_orders_df)
        cancel_flag = 0

    return buy_price_list, cancel_flag


def open_buy_orders_grid(exchange, bot_name, config_params, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    base_currency, quote_currency = get_currency(config_params)
    bid_price = get_bid_price(exchange, config_params)
    print(f"Bid price: {bid_price:.2f} {quote_currency}")
    
    remain_budget, free_budget = cal_budget(config_params, open_orders_df_path)
    buy_price_list, cancel_flag = cal_buy_price_list(remain_budget, free_budget, bid_price, config_params, open_orders_df_path)
    
    if cancel_flag == 1:
        cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)

    print(f"Open {len(buy_price_list)} buy orders")

    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    remain_cash_flow_accum = sum(cash_flow_df['remain_cash_flow'])

    for price in buy_price_list:
        amount = config_params['value'] / price
        floor_amount = round_down_amount(amount, config_params)
        
        quote_currency_free = get_quote_currency_free(exchange, quote_currency)

        if quote_currency_free >= remain_cash_flow_accum + config_params['value']:
            buy_order = exchange.create_order(config_params['symbol'], 'limit', 'buy', floor_amount, price, params={'postOnly':True})
            append_order(buy_order, 'amount', open_orders_df_path)
            print(f"Open buy {floor_amount:.3f} {base_currency} at {price:.2f} {quote_currency}")
        else:
            # Actual buget less than cal_budget (could caused by open_orders match during loop).
            print(f"Error: Cannot buy at price {price:.2f} {quote_currency} due to insufficient fund!!!")
            break

        
def open_sell_orders_grid(buy_order, exchange, config_params, open_orders_df_path, error_log_df_path):
    base_currency, quote_currency = get_currency(config_params)
    ask_price = get_ask_price(exchange, config_params)
    sell_price = cal_sell_price(buy_order, ask_price, config_params)
    
    try:
        final_amount = cal_final_amount(buy_order['id'], exchange, base_currency, config_params)
        sell_order = exchange.create_order(config_params['symbol'], 'limit', 'sell', final_amount, sell_price)
        append_order(sell_order, 'amount', open_orders_df_path)
    except ccxt.InsufficientFunds:
        # Not available amount to sell (could caused by decimal), sell free amount.
        base_currency_free = get_base_currency_free(exchange, base_currency)
        final_amount = round_down_amount(base_currency_free, config_params)
        sell_order = exchange.create_order(config_params['symbol'], 'limit', 'sell', final_amount, sell_price)
        append_error_log('InsufficientFunds', error_log_df_path)
    except ccxt.InvalidOrder:
        # Filled small value than minimum order, ignore.
        sell_order = None
        append_error_log('InvalidOrder', error_log_df_path)
    
    print(f"Open sell {final_amount:.3f} {base_currency} at {sell_price:.2f} {quote_currency}")
    return sell_order


def clear_orders_grid(side, exchange, bot_name, config_params, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()

    if side == 'sell':
        # Buy orders: FIFO.
        # Sell orders: LIFO.
        open_orders_list.reverse()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, config_params['symbol'])
        
        if order['status'] == 'closed':
            noti_success_order(order, bot_name, config_params)

            if side == 'buy':
                open_sell_orders_grid(order, exchange, config_params, open_orders_df_path, error_log_df_path)

            remove_order(order_id, open_orders_df_path)
            append_order(order, 'filled', transactions_df_path)

        elif order['status'] == 'canceled':
            # Canceld by param PostOnly.
            remove_order(order_id, open_orders_df_path)


def cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()
    
    if len(open_buy_orders_list) > 0:
        for order_id in open_buy_orders_list:
            order = exchange.fetch_order(order_id, config_params['symbol'])
            
            try:
                exchange.cancel_order(order_id, config_params['symbol'])
                print(f"Cancel order {order_id}")
                
                if order['filled'] > 0:
                    append_order(order, 'filled', transactions_df_path)
                    open_sell_orders_grid(order, exchange, config_params, open_orders_df_path, error_log_df_path)
                
                remove_order(order_id, open_orders_df_path)
            except ccxt.OrderNotFound:
                # No order in the system (could casued by the order is queued), skip for the next loop.
                append_error_log('OrderNotFound', error_log_df_path)
                print(f"Error: Cannot cancel order {order_id} due to unavailable order!!!")
            except ccxt.InvalidOrder:
                # The order is closed by system (could caused by post_only param for buy orders).
                append_error_log('SizeTooSmall', error_log_df_path)
                remove_order(order_id, open_orders_df_path)


def check_circuit_breaker(exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path):
    cont_flag = 1
    
    _, quote_currency = get_currency(config_params)
    last_loop = get_json(last_loop_path)
    transactions_df = pd.read_csv(transactions_df_path)
    update_last_loop_price(exchange, config_params, last_loop_path)

    if len(transactions_df) >= config_params['circuit_limit']:
        side_list = transactions_df['side'][-config_params['circuit_limit']:].unique()
        
        last_price = get_last_price(exchange, config_params)

        if (len(side_list) == 1) & (side_list[0] == 'buy') & (last_price <= last_loop['price']):
            cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
            noti_warning(f"Circuit breaker at {last_price:.2f} {quote_currency}", bot_name)
            time.sleep(config_system['idle_rest'])

    return cont_flag


def check_cut_loss(exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, cash_flow_df_path):
    cont_flag = 1

    _, quote_currency = get_currency(config_params)
    quote_currency_free = get_quote_currency_free(exchange, quote_currency)

    open_orders_df = pd.read_csv(open_orders_df_path)
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    min_sell_price = min(open_orders_df['price'], default=0)    

    transfer = get_json(transfer_path)
    available_cash_flow = get_available_cash_flow(cash_flow_df)

    last_price = get_last_price(exchange, config_params)

    if (quote_currency_free - available_cash_flow - transfer['withdraw_cash_flow'] < config_params['value']) & ((min_sell_price - last_price) >= (config_params['grid'] * 2)):
        cont_flag = 0
        
        while quote_currency_free - available_cash_flow - transfer['withdraw_cash_flow'] < config_params['value']:
            cut_loss(exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, open_orders_df_path, withdraw_flag=False)
            quote_currency_free = get_quote_currency_free(exchange, quote_currency)

    return cont_flag
            

def update_loss(loss, last_loop_path):
    last_loop = get_json(last_loop_path)
    total_loss = last_loop['loss']
    total_loss -= loss
    last_loop['loss'] = total_loss

    update_json(last_loop, last_loop_path)


def reduce_budget(loss, config_params_path):
    config_params = get_json(config_params_path)
    
    budget = config_params['budget']
    # Loss is negative.
    budget += loss
    config_params['budget'] = budget

    update_json(config_params, config_params_path)


def cut_loss(exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, open_orders_df_path, withdraw_flag):
    open_orders_df = pd.read_csv(open_orders_df_path)
    max_sell_price = max(open_orders_df['price'])
    canceled_df = open_orders_df[open_orders_df['price'] == max_sell_price]

    _, quote_currency = get_currency(config_params)

    canceled_id = canceled_df['order_id'].reset_index(drop=True)[0]
    buy_amount = canceled_df['amount'].reset_index(drop=True)[0]
    buy_price = max_sell_price - config_params['grid']
    buy_value = buy_price * buy_amount

    try:
        exchange.cancel_order(canceled_id, config_params['symbol'])
        time.sleep(config_system['idle_stage'])
        canceled_order = exchange.fetch_order(canceled_id, config_params['symbol'])

        while canceled_order['status'] != 'canceled':
            # Cancel orders will be removed from db on the next loop by check_orders_status.
            time.sleep(config_system['idle_stage'])
            canceled_order = exchange.fetch_order(canceled_id, config_params['symbol'])

        remove_order(canceled_id, open_orders_df_path)

        sell_order = exchange.create_order(config_params['symbol'], 'market', 'sell', buy_amount)
        time.sleep(config_system['idle_stage'])

        while sell_order['status'] != 'closed':
            time.sleep(config_system['idle_stage'])
            sell_order = exchange.fetch_order(sell_order['id'], config_params['symbol'])

        new_sell_price = sell_order['price']
        new_sell_amount = sell_order['amount']
        new_sell_value = new_sell_price * new_sell_amount
        loss = new_sell_value - buy_value
        
        update_loss(loss, last_loop_path)
        reduce_budget(loss, config_params_path)
        noti_warning(f"Cut loss {loss:.2f} {quote_currency} at {new_sell_price:.2f} {quote_currency}", bot_name)

        if withdraw_flag == False:
            time.sleep(config_system['idle_rest'])
    
    except ccxt.InvalidOrder:
        # Order has already been canceled from last loop but failed to update open_orders_df.
        remove_order(canceled_id, open_orders_df_path)


def reset_loss(last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['loss'] = 0

    update_json(last_loop, last_loop_path)


def update_reinvest(init_budget, new_budget, new_value, config_params_path):
    config_params = get_json(config_params_path)
    config_params['init_budget'] = init_budget
    config_params['budget'] = new_budget
    config_params['value'] = new_value

    update_json(config_params, config_params_path)


def update_budget_grid(prev_date, exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transactions_df = pd.read_csv(transactions_df_path)
    last_loop = get_json(last_loop_path)

    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)
    balance_value = current_value + cash
    
    unrealised, _, _, _ = cal_unrealised(last_price, config_params, open_orders_df)
    base_currency_free = get_base_currency_free(exchange, base_currency)

    last_sell_df = last_transactions_df[last_transactions_df['side'] == 'sell']
    cash_flow = sum(last_sell_df['amount'] * config_params['grid'])

    commission = max(cash_flow * config_params['commission_rate'], 0)
    net_cash_flow = cash_flow - commission
    
    if config_params['reinvest_ratio'] == -1:
        greed_index = get_greed_index()
        reinvest_ratio = max(1 - (greed_index / 100), 0)

    reinvest_amount = net_cash_flow * reinvest_ratio
    remain_cash_flow = net_cash_flow - reinvest_amount

    transfer = get_json(transfer_path)
    lower_price = last_price * (1 - config_params['fluctuation_rate'])
    n_order = int((last_price - lower_price) / config_params['grid'])

    net_transfer = transfer['deposit'] - transfer['withdraw']
    new_init_budget = config_params['init_budget'] + net_transfer
    new_budget = config_params['budget'] + reinvest_amount + net_transfer
    new_value = new_budget / n_order
    
    available_cash_flow = get_available_cash_flow(cash_flow_df)
    available_cash_flow += (remain_cash_flow - transfer['withdraw_cash_flow'])
    available_yield = get_available_yield(cash_flow_df)
    available_yield += (commission - transfer['withdraw_yield'])

    quote_currency_free = get_quote_currency_free(exchange, quote_currency)
    
    while quote_currency_free - available_cash_flow - available_yield < -net_transfer:
        cut_loss(exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, open_orders_df_path, withdraw_flag=True)
        quote_currency_free = get_quote_currency_free(exchange, quote_currency)

    cash_flow_list = [
        prev_date,
        balance_value,
        unrealised,
        base_currency_free,
        last_loop['loss'],
        config_params['budget'],
        config_params['value'],
        cash_flow,
        commission,
        net_cash_flow,
        reinvest_amount,
        remain_cash_flow,
        transfer['deposit'],
        transfer['withdraw'],
        transfer['withdraw_cash_flow'],
        transfer['withdraw_yield'],
        available_cash_flow,
        available_yield
        ]

    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_reinvest(new_init_budget, new_budget, new_value, config_params_path)
    reset_loss(last_loop_path)
    reset_transfer(transfer_path)


def print_report_grid(exchange, config_params, open_orders_df_path):
    base_currency, quote_currency = get_currency(config_params)
    last_price = get_last_price(exchange, config_params)

    print_current_balance(last_price, exchange, config_params)
    print(f"Last price: {last_price:.2f} {quote_currency}")
    print_hold_assets(last_price, base_currency, quote_currency, config_params, open_orders_df_path)
    print_pending_order(quote_currency, open_orders_df_path)