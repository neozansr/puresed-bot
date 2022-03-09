import pandas as pd
import json

from func_get import get_json, get_time, get_last_price


def update_json(input_dict, file_path):
    with open(file_path, 'w') as file:
        json.dump(input_dict, file, indent=4)


def append_csv(value_list, df, df_path):
    df.loc[len(df)] = value_list
    df.to_csv(df_path, index=False)


def append_order(order, amount_key, df_path):
    '''
    Add success order record.
    '''
    df = pd.read_csv(df_path)

    timestamp = get_time()
    
    if order['price'] == None:
        # New created market orders.
        # Used for open_orders_df only.
        value = None
    else:
        value = order['cost']

    df.loc[len(df)] = [timestamp, order['id'], order['symbol'], order['type'], order['side'], order[amount_key], order['price'], value]
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


def update_last_loop_price(exchange, symbol, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_price = get_last_price(exchange, symbol)
    last_loop['price'] = last_price

    update_json(last_loop, last_loop_path)


def update_timestamp(last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['timestamp'] = str(get_time())

    update_json(last_loop, last_loop_path)


def update_transfer(taker_fee_percent, transfer_path):
    '''
    Update transfer when withdraw happens
    Change withdraw amount to pending withdraw
    Set others to zero
    '''
    transfer = get_json(transfer_path)
    withdraw = transfer['withdraw']

    transfer['deposit'] = 0
    transfer['withdraw'] = 0

    fee = withdraw * (taker_fee_percent / 100)
    adjusted_withdraw = withdraw - fee
    transfer['pending_withdraw'] = transfer['pending_withdraw'] + adjusted_withdraw

    update_json(transfer, transfer_path)