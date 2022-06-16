import ccxt
import numpy as np
import pandas as pd
import time

import func_get
import func_cal
import func_update
import func_noti


def get_cash_flow_grid(date, config_params, transactions_df_path):
    transactions_df = pd.read_csv(transactions_df_path)
    ref_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == date]
    last_sell_df = ref_transactions_df[(ref_transactions_df['side'] == 'sell') & (ref_transactions_df['remark'].isin(['close_order', 'close_incomplete']))]
    cash_flow = sum(last_sell_df['amount'] * config_params['grid'])

    return cash_flow


def cal_unrealised_grid(last_price, grid, open_orders_df):
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    n_open_sell_oders = len(open_sell_orders_df)
    
    price_list = [x - grid for x in open_sell_orders_df['price']]
    amount_list = open_sell_orders_df['amount'].to_list()

    amount = sum(amount_list)
    total_value = sum([i * j for i, j in zip(price_list, amount_list)])
    
    try:
        avg_price = total_value / amount
    except ZeroDivisionError:
        avg_price = 0

    unrealised = (last_price - avg_price) * amount

    return unrealised, n_open_sell_oders, amount, avg_price


def cal_buy_price_list(remain_buy_orders, bid_price, open_orders_df_path, config_params):
    # Update open_orders_df before cal price.
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    min_open_sell_price = min(open_sell_orders_df['price'], default=np.inf)

    if len(open_buy_orders_df) > 0:
        start_buy_price = min(open_buy_orders_df['price']) - config_params['grid']
    else:
        if len(open_sell_orders_df) == 0:
            start_buy_price = bid_price - (config_params['grid'] * config_params['start_safety'])
        else:
            # Use (grid * 2) to prevent dupplicate order.
            start_buy_price = min(min_open_sell_price - (config_params['grid'] * 2), bid_price)
    
    buy_price = min(start_buy_price, config_params['max_price'])
    buy_price_list = []

    while (buy_price > config_params['min_price']) & (len(buy_price_list) < remain_buy_orders):
        buy_price_list.append(buy_price)
        buy_price -= config_params['grid']

    return buy_price_list


def open_buy_orders_grid(exchange, config_params, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transfer = func_get.get_json(transfer_path)

    base_currency, quote_currency = func_get.get_currency(config_params['symbol'])

    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    max_open_buy_price = max(open_buy_orders_df['price'], default=0)
    min_open_sell_price = min(open_sell_orders_df['price'], default=np.inf)

    bid_price = func_get.get_bid_price(exchange, config_params['symbol'])
    print(f"Bid price: {bid_price} {quote_currency}")
    
    price_gap = min(bid_price, min_open_sell_price - config_params['grid']) - max_open_buy_price
    if  price_gap > (config_params['grid'] * 1.01):
        # Grid * 1.01 to prevent small decimal diff by python.
        print(f"Price gap {price_gap} {quote_currency}")
        cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
        n_open_buy_orders = 0
    else:
        n_open_buy_orders = len(open_buy_orders_df)

    if config_params['circuit_limit'] == 0:
        n_limit_order = np.inf
    else:    
        n_limit_order = config_params['circuit_limit']

    remain_buy_orders = max(n_limit_order - n_open_buy_orders, 0)
    buy_price_list = cal_buy_price_list(remain_buy_orders, bid_price, open_orders_df_path, config_params)
    
    cur_date = func_get.get_date()
    cash_flow = get_cash_flow_grid(cur_date, config_params, transactions_df_path)
    funding_payment = func_get.get_funding_payment(exchange, range='today')
    reserve = func_get.get_reserve(transfer, cash_flow_df)

    print(f"Open {len(buy_price_list)} buy orders")
    
    for price in buy_price_list:
        available_cash = func_cal.cal_available_cash(exchange, cash_flow, funding_payment, reserve, config_params, transfer)

        amount = config_params['value'] / price
        amount = func_cal.round_amount(amount, exchange, config_params['symbol'], round_direction='down')

        if available_cash >= config_params['value']:
            buy_order = exchange.create_order(config_params['symbol'], 'limit', 'buy', amount, price, params={'postOnly':True})
            func_update.append_order(buy_order, 'amount', 'open_order', open_orders_df_path)
            print(f"Open buy {amount} {base_currency} at {price} {quote_currency}")
        else:
            print(f"Error: Cannot buy at price {price} {quote_currency} due to insufficient fund!!!")
            break


def cal_sell_price(order, ask_price, config_params):
    sell_price = max(order['price'] + config_params['grid'], ask_price)

    return sell_price

    
def open_sell_orders_grid(buy_order, exchange, config_params, open_orders_df_path, error_log_df_path):
    base_currency, quote_currency = func_get.get_currency(config_params['symbol'])
    ask_price = func_get.get_ask_price(exchange, config_params['symbol'])
    sell_price = cal_sell_price(buy_order, ask_price, config_params)
    
    try:
        sell_amount = buy_order['filled']
        sell_order = exchange.create_order(config_params['symbol'], 'limit', 'sell', sell_amount, sell_price)
        func_update.append_order(sell_order, 'amount', 'open_order', open_orders_df_path)
    except (ccxt.InvalidOrder, ccxt.InsufficientFunds):
        # InvalidOrder: The order has already been closed by postOnly param.
        # InsufficientFunds: Not available amount to sell cause by fee deduction.
        func_update.append_error_log(f"CannotOpenSell {buy_order['id']}", error_log_df_path)
    
    print(f"Open sell {sell_amount} {base_currency} at {sell_price} {quote_currency}")


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
            func_noti.noti_success_order(order, bot_name, config_params['symbol'])

            if side == 'buy':
                open_sell_orders_grid(order, exchange, config_params, open_orders_df_path, error_log_df_path)

            func_update.remove_order(order_id, open_orders_df_path)
            func_update.append_order(order, 'filled', 'close_order', transactions_df_path)

        elif order['status'] == 'canceled':
            # Canceld by param PostOnly.
            func_update.remove_order(order_id, open_orders_df_path)


def clear_free_base_currency(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path):
    free_amount = func_get.get_base_currency_free(exchange, config_params['symbol'], open_orders_df_path)
    clear_amount = func_cal.round_amount(free_amount, exchange, config_params['symbol'], round_direction='down')

    if clear_amount > 0:
        # Free amount more than minimum order, sell all.
        clear_order = exchange.create_order(config_params['symbol'], 'market', 'sell', clear_amount)

        while clear_order['status'] != 'closed':
            time.sleep(config_system['idle_stage'])
            clear_order = exchange.fetch_order(clear_order['id'], config_params['symbol'])

        func_noti.noti_clear_order(clear_order, bot_name, config_params['symbol'])
        func_update.append_order(clear_order, 'filled', 'clear_free', transactions_df_path)


def cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()
    
    if len(open_buy_orders_list) > 0:
        for order_id in open_buy_orders_list:
            order = exchange.fetch_order(order_id, config_params['symbol'])
            
            try:
                exchange.cancel_order(order_id)
                print(f"Cancel order {order_id}")
                
                if order['filled'] > 0:
                    func_update.append_order(order, 'filled', 'close_incomplete', transactions_df_path)
                    open_sell_orders_grid(order, exchange, config_params, open_orders_df_path, error_log_df_path)
                
                func_update.remove_order(order_id, open_orders_df_path)
            except ccxt.OrderNotFound:
                # No order in the system (could casued by the order is queued), skip for the next loop.
                func_update.append_error_log('OrderNotFound', error_log_df_path)
                print(f"Error: Cannot cancel order {order_id} due to unavailable order!!!")
            except ccxt.InvalidOrder:
                # The order is closed by system (could caused by post_only param for buy orders).
                func_update.append_error_log(f'InvalidOrder', error_log_df_path)
                func_update.remove_order(order_id, open_orders_df_path)


def check_circuit_breaker(exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path):
    cont_flag = True

    if config_params['circuit_limit'] == 0:
        circuit_limit = np.inf
    else:
        circuit_limit = config_params['circuit_limit']
    
    _, quote_currency = func_get.get_currency(config_params['symbol'])
    last_loop = func_get.get_json(last_loop_path)
    transactions_df = pd.read_csv(transactions_df_path)
    func_update.update_last_loop_price(exchange, config_params['symbol'], last_loop_path)

    if len(transactions_df) >= circuit_limit:
        side_list = transactions_df['side'][-circuit_limit:].unique()
        last_price = func_get.get_last_price(exchange, config_params['symbol'])

        if (len(side_list) == 1) & (side_list[0] == 'buy') & (last_price <= last_loop['price']):
            cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
            func_noti.noti_warning(f"Circuit breaker at {last_price} {quote_currency}", bot_name)
            time.sleep(config_system['idle_rest'])

    return cont_flag
        

def update_loss(loss, last_loop_path):
    last_loop = func_get.get_json(last_loop_path)
    last_loop['loss'] -= loss

    func_update.update_json(last_loop, last_loop_path)


def reset_loss(last_loop_path):
    last_loop = func_get.get_json(last_loop_path)
    last_loop['loss'] = 0

    func_update.update_json(last_loop, last_loop_path)


def cut_loss(exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path, withdraw_flag):
    open_orders_df = pd.read_csv(open_orders_df_path)
    max_sell_price = max(open_orders_df['price'])
    canceled_df = open_orders_df[open_orders_df['price'] == max_sell_price]

    _, quote_currency = func_get.get_currency(config_params['symbol'])

    canceled_id = canceled_df['order_id'].reset_index(drop=True)[0]
    buy_amount = canceled_df['amount'].reset_index(drop=True)[0]
    buy_price = max_sell_price - config_params['grid']
    buy_value = buy_price * buy_amount

    try:
        exchange.cancel_order(canceled_id)
        time.sleep(config_system['idle_stage'])
        canceled_order = exchange.fetch_order(canceled_id, config_params['symbol'])

        while canceled_order['status'] != 'canceled':
            time.sleep(config_system['idle_stage'])
            canceled_order = exchange.fetch_order(canceled_id, config_params['symbol'])

        func_update.remove_order(canceled_id, open_orders_df_path)

        sell_order = exchange.create_order(config_params['symbol'], 'market', 'sell', buy_amount)
        time.sleep(config_system['idle_stage'])

        while sell_order['status'] != 'closed':
            time.sleep(config_system['idle_stage'])
            sell_order = exchange.fetch_order(sell_order['id'], config_params['symbol'])

        func_update.append_order(sell_order, 'filled', 'cut_loss', transactions_df_path)
            
        fee = func_get.get_order_fee(sell_order, exchange, config_params['symbol'], config_system) 
        cut_loss_value = sell_order['amount'] * sell_order['price']
        loss = cut_loss_value - buy_value - fee
        
        update_loss(loss, last_loop_path)
        func_noti.noti_warning(f"Cut loss {loss} {quote_currency} at {cut_loss_value} {quote_currency}", bot_name)

        if withdraw_flag == False:
            time.sleep(config_system['idle_rest'])
    
    except ccxt.InvalidOrder:
        # Order has already been canceled from last loop but failed to update df.
        func_update.append_error_log(f'InvalidOrder:LastLoopClose', error_log_df_path)
        func_update.remove_order(canceled_id, open_orders_df_path)


def update_value(config_params, config_params_path):
    max_n_order = (config_params['max_price'] - config_params['min_price']) / config_params['grid']
    config_params['value'] = config_params['budget'] / max_n_order

    func_update.update_json(config_params, config_params_path)


def update_end_date_grid(prev_date, exchange, bot_name, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    last_loop = func_get.get_json(last_loop_path)

    last_price = func_get.get_last_price(exchange, config_params['symbol'])
    base_currency_free = func_get.get_base_currency_free(exchange, config_params['symbol'], open_orders_df_path)
    
    cash_flow = get_cash_flow_grid(prev_date, config_params, transactions_df_path)
    funding_payment, _ = func_get.get_funding_payment(exchange, range='end_date')
    net_cash_flow = cash_flow - funding_payment

    transfer = func_get.get_json(transfer_path)
    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer != 0:
        cancel_open_buy_orders_grid(exchange, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
    
    reserve = func_get.get_reserve(transfer, cash_flow_df)
    reserve += net_cash_flow

    cur_date = func_get.get_date()
    cash_flow = get_cash_flow_grid(cur_date, config_params, transactions_df_path)
    available_cash = func_cal.cal_available_cash(exchange, cash_flow, funding_payment, reserve, config_params, transfer)

    # Cut loss until quote_currency_free is enough to withdraw.
    while available_cash < -net_transfer:
        cut_loss(exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path, withdraw_flag=True)
        available_cash = func_cal.cal_available_cash(exchange, cash_flow, funding_payment, reserve, config_params, transfer)

    if '-PERP' in config_params['symbol']:
        current_value = 0
    else:
        current_value = func_get.get_base_currency_value(last_price, exchange, config_params['symbol'])
    
    cash = func_get.get_quote_currency_value(exchange, config_params['symbol'])
    end_balance = func_cal.cal_end_balance(current_value, cash, transfer)
    unrealised, _, _, _ = cal_unrealised_grid(last_price, config_params['grid'], open_orders_df)

    cash_flow_list = [
        prev_date,
        config_params['grid'],
        config_params['value'],
        config_params['budget'],
        end_balance,
        unrealised,
        last_loop['loss'],
        cash_flow,
        funding_payment,
        net_cash_flow,
        base_currency_free,
        transfer['deposit'],
        transfer['withdraw'],
        transfer['withdraw_reserve'],
        reserve
        ]

    func_update.append_csv(cash_flow_list, cash_flow_df, cash_flow_df_path)
    reset_loss(last_loop_path)
    update_value(config_params, config_params_path)
    func_update.update_transfer(config_system['taker_fee_percent'], transfer_path)


def print_current_balance(last_price, exchange, symbol):
    _, quote_currency = func_get.get_currency(symbol)

    if '-PERP' in symbol:
        current_value = 0
    else:
        current_value = func_get.get_base_currency_value(last_price, exchange, symbol)
    
    cash = func_get.get_quote_currency_value(exchange, symbol)
    balance_value = current_value + cash
    
    print(f"Balance: {balance_value} {quote_currency}")


def print_hold_assets(last_price, base_currency, quote_currency, grid, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised_grid(last_price, grid, open_orders_df)

    assets_dict = {
        'timestamp': func_get.get_time(),
        'last_price': last_price, 
        'avg_price': avg_price, 
        'amount': amount, 
        'unrealised': unrealised
        }

    assets_df = pd.DataFrame(assets_dict, index=[0])
    assets_df.to_csv('assets.csv', index=False)
    
    print(f"Hold {amount} {base_currency} with {n_open_sell_oders} orders at {avg_price} {quote_currency}")
    print(f"Unrealised: {unrealised} {quote_currency}")


def print_pending_order(quote_currency, open_orders_df_path):
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = func_get.get_pending_order(open_orders_df_path)

    print(f"Min buy price: {min_buy_price} {quote_currency}")
    print(f"Max buy price: {max_buy_price} {quote_currency}")
    print(f"Min sell price: {min_sell_price} {quote_currency}")
    print(f"Max sell price: {max_sell_price} {quote_currency}")


def print_report_grid(exchange, config_params, open_orders_df_path):
    base_currency, quote_currency = func_get.get_currency(config_params['symbol'])
    last_price = func_get.get_last_price(exchange, config_params['symbol'])
    
    print_current_balance(last_price, exchange, config_params['symbol'])
    print(f"Last price: {last_price} {quote_currency}")
    print_hold_assets(last_price, base_currency, quote_currency, config_params['grid'], open_orders_df_path)
    print_pending_order(quote_currency, open_orders_df_path)