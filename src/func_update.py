import pandas as pd
import json

from func_get import get_time, get_last_price


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


def append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path):
    cash_flow_df.loc[len(cash_flow_df)] = cash_flow_list
    cash_flow_df.to_csv(cash_flow_df_path, index=False)


def update_last_loop_price(exchange, symbol, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    last_price = get_last_price(exchange, symbol)
    last_loop['price'] = last_price

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def reset_transfer(transfer_path):
    with open(transfer_path) as transfer_file:
        transfer = json.load(transfer_file)

    for s in transfer.keys():
        transfer[s] = 0

    with open(transfer_path, 'w') as transfer_file:
        json.dump(transfer, transfer_file, indent=1)