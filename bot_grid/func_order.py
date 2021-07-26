import ccxt
import pandas as pd
import datetime as dt
import time

from func_get import get_time, get_bid_price, get_ask_price, get_last_loop_price, get_transfer, get_available_cash_flow
from func_get import update_last_loop_price, update_loss, reduce_budget
from func_cal import round_down_amount, cal_final_amount, cal_sell_price, cal_buy_price_list
from func_noti import noti_success_order, noti_warning


def append_order(amount_key, order, config_params, df_path):
    df = pd.read_csv(df_path)

    timestamp = get_time()
    order_id = order['id']
    order_type = order['type']
    order_side = order['side']
    amount = order[amount_key]
    price = order['price']
    value = amount * price

    df.loc[len(df)] = [timestamp, order_id, config_params['symbol'], order_type, order_side, amount, price, value]
    df.to_csv(df_path, index=False)


def remove_order(order_id, df_path):
    df = pd.read_csv(df_path)

    df = df[df['order_id'] != order_id]
    df = df.reset_index(drop=True)
    df.to_csv(df_path, index=False)


def append_error_log(error_log, error_log_df_path):
    df = pd.read_csv(error_log_df_path)
    
    timestamp = get_time()
    df.loc[len(df)] = [timestamp, error_log]
    df.to_csv(error_log_df_path, index=False)


def open_sell_order(buy_order, exchange, base_currency, quote_currency, config_system, config_params, error_log_df_path):
    ask_price = get_ask_price(exchange, config_params)
    sell_price = cal_sell_price(buy_order, ask_price, config_params)
    
    try:
        final_amount = cal_final_amount(buy_order['id'], exchange, config_system, config_params)
        sell_order = exchange.create_order(config_params['symbol'], 'limit', 'sell', final_amount, sell_price)
    except ccxt.InsufficientFunds:
        # not available amount to sell (could caused by decimal), sell free amount
        balance = exchange.fetch_balance()
        base_currency_amount = balance[base_currency]['free']
        final_amount = round_down_amount(base_currency_amount, config_params)
        sell_order = exchange.create_order(config_params['symbol'], 'limit', 'sell', final_amount, sell_price)
        append_error_log('InsufficientFunds', error_log_df_path)
    except ccxt.InvalidOrder:
        # filled small value than minimum order, ignore
        append_error_log('InvalidOrder', error_log_df_path)
    
    print(f'Open sell {final_amount:.3f} {base_currency} at {sell_price} {quote_currency}')
    return sell_order


def check_orders_status(side, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_orders_list = open_orders_df[open_orders_df['side'] == side]['order_id'].to_list()

    if side == 'sell':
        # buy orders: FIFO
        # sell orders: LIFO
        open_orders_list.reverse()
    
    for order_id in open_orders_list:
        order = exchange.fetch_order(order_id, config_params['symbol'])
        
        if order['status'] == 'closed':
            noti_success_order(order, bot_name, base_currency, quote_currency)

            if side == 'buy':
                sell_order = open_sell_order(order, exchange, base_currency, quote_currency, config_system, config_params, error_log_df_path)
                append_order('amount', sell_order, config_params, open_orders_df_path)

            remove_order(order_id, open_orders_df_path)
            append_order('filled', order, config_params, transactions_df_path)

        elif order['status'] == 'canceled':
            # canceld by param PostOnly
            remove_order(order_id, open_orders_df_path)


def cancel_open_buy_orders(exchange, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    open_buy_orders_list = open_buy_orders_df['order_id'].to_list()
    
    if len(open_buy_orders_list) > 0:
        for order_id in open_buy_orders_list:
            order = exchange.fetch_order(order_id, config_params['symbol'])
            filled = order['filled']
            
            try:
                exchange.cancel_order(order_id, config_params['symbol'])
                print(f'Cancel order {order_id}')
                
                if filled > 0:
                    append_order('filled', order, config_params, transactions_df_path)
                    
                    sell_order = open_sell_order(order, exchange, base_currency, quote_currency, config_system, config_params, error_log_df_path)
                    append_order('amount', sell_order, config_params, open_orders_df_path)
                
                remove_order(order_id, open_orders_df_path)
            except ccxt.OrderNotFound:
                # no order in the system (could casued by the order is queued), skip for the next loop
                append_error_log('OrderNotFound', error_log_df_path)
                print(f'Error: Cannot cancel order {order_id} due to unavailable order!!!')
            except ccxt.InvalidOrder:
                # the order is closed by system (could caused by post_only param for buy orders)
                remove_order(open_orders_df_path, order_id)


def open_buy_orders(remain_budget, free_budget, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path, cash_flow_df_path):
    bid_price = get_bid_price(exchange, config_params)
    buy_price_list, cancel_flag = cal_buy_price_list(remain_budget, free_budget, bid_price, config_params, open_orders_df_path)
    
    if cancel_flag == 1:
        cancel_open_buy_orders(exchange, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)

    print(f'Open {len(buy_price_list)} buy orders')

    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    remain_cash_flow_accum = sum(cash_flow_df['remain_cash_flow'])

    for price in buy_price_list:
        amount = config_params['value'] / price
        floor_amount = round_down_amount(amount, config_params['decimal'])
        
        balance = exchange.fetch_balance()
        quote_currency_amount = balance[quote_currency]['free']

        if quote_currency_amount >= remain_cash_flow_accum + config_params['value']:
            buy_order = exchange.create_order(config_params['symbol'], 'limit', 'buy', floor_amount, price, params={'postOnly':True})
            append_order('amount', buy_order, config_params, open_orders_df_path)
            print(f'Open buy {floor_amount:.3f} {base_currency} at {price} {quote_currency}')
        else:
            # actual buget less than cal_budget (could caused by open_orders match during loop)
            print(f'Error: Cannot buy at price {price} {quote_currency} due to insufficient fund!!!')
            break


def check_circuit_breaker(exchange, bot_name, base_currency, quote_currency, last_price, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, error_log_df_path):
    cont_flag = 1

    last_loop_price = get_last_loop_price(last_loop_path)
    transactions_df = pd.read_csv(transactions_df_path)
    update_last_loop_price(exchange, config_params['symbol'], last_loop_path)

    if len(transactions_df) >= config_params['circuit_limit']:
        side_list = transactions_df['side'][-config_params['circuit_limit']:].unique()
        
        if (len(side_list) == 1) & (side_list[0] == 'buy'):
            if last_price <= last_loop_price:
                cont_flag = 0

                cancel_open_buy_orders(exchange, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path, error_log_df_path)
                noti_warning(bot_name, f'Circuit breaker at {last_price} {quote_currency}')
                time.sleep(config_system['idle_rest'])

    return cont_flag


def check_cut_loss(exchange, bot_name, quote_currency, last_price, config_system, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, cash_flow_df_path):
    cont_flag = 1

    balance = exchange.fetch_balance()
    quote_currency_amount = balance[quote_currency]['free']

    open_orders_df = pd.read_csv(open_orders_df_path)
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    min_sell_price = min(open_orders_df['price'], default=0)    

    _, _, withdraw_cash_flow = get_transfer(transfer_path)
    available_cash_flow = get_available_cash_flow(withdraw_cash_flow, cash_flow_df)

    if quote_currency_amount < available_cash_flow + config_params['value']:
        if (min_sell_price - last_price) >= (config_params['grid'] * 2):
            cont_flag = 0
            
            while quote_currency_amount < available_cash_flow + config_params['value']:
                cut_loss(exchange, bot_name, quote_currency, last_price, config_system, config_params, config_params_path, last_loop_path, open_orders_df_path)

                balance = exchange.fetch_balance()
                quote_currency_amount = balance[quote_currency]['free']

    return cont_flag
            

def cut_loss(exchange, bot_name, quote_currency, last_price, config_system, config_params, config_params_path, last_loop_path, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    max_sell_price = max(open_orders_df['price'])
    canceled_df = open_orders_df[open_orders_df['price'] == max_sell_price]

    canceled_id = canceled_df['order_id'].reset_index(drop=True)[0]
    buy_amount = canceled_df['amount'].reset_index(drop=True)[0]
    buy_price = max_sell_price - config_params['grid']
    buy_value = buy_price * buy_amount

    try:
        exchange.cancel_order(canceled_id, config_params['symbol'])
        time.sleep(config_system['idle_stage'])
        canceled_order = exchange.fetch_order(canceled_id, config_params['symbol'])

        while canceled_order['status'] != 'canceled':
            # cancel orders will be removed from db on the next loop by check_orders_status
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
        noti_warning(f'Cut loss {loss:.2f} {quote_currency} at {last_price} {quote_currency}', bot_name)

        time.sleep(config_system['idle_rest'])
    
    except ccxt.InvalidOrder:
        # order has already been canceled from last loop but failed to update open_orders_df
        remove_order(canceled_id, open_orders_df_path)