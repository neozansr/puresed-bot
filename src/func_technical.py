import numpy as np
import pandas as pd
import math
import time
import json

from func_get import get_time, convert_tz, get_last_price, get_balance, get_last_loop, get_transfer
from func_cal import round_down_amount, round_up_amount
from func_update import append_order, remove_order, append_cash_flow_df, reset_transfer
from func_noti import noti_success_order


def get_timeframe(config_params):
    timeframe_dict = {1440:'1d', 240:'4h', 60:'1h', 30:'30m', 15:'15m'}
    
    for i in timeframe_dict.keys():
        if config_params['interval'] % i == 0:
            min_interval = i
            break
            
    min_timeframe = timeframe_dict[min_interval]
    step = int(config_params['interval'] / min_interval)
            
    return min_timeframe, step


def get_ohlcv(exchange, config_params):
    min_timeframe, step = get_timeframe(config_params)

    ohlcv = exchange.fetch_ohlcv(config_params['symbol'], timeframe=min_timeframe, limit=config_params['window'])
    ohlcv_df = pd.DataFrame(ohlcv)
    ohlcv_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    ohlcv_df['time'] = pd.to_datetime(ohlcv_df['time'], unit='ms')
    ohlcv_df['time'] = ohlcv_df['time'].apply(lambda x: convert_tz(x))

    last_timestamp = str(ohlcv_df.loc[len(ohlcv_df) - 1, 'time'])

    return ohlcv_df, last_timestamp


def update_timestamp(last_timestamp, last_loop_path):
    last_loop = get_last_loop(last_loop_path)
    last_loop['timestamp'] = last_timestamp

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_side(side, last_loop_path):
    last_loop = get_last_loop(last_loop_path)
    last_loop['side'] = side

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def signal_tma(ohlcv_df, config_params):
    sub_interval = (config_params['window'] + 1) / 2
    
    # trunc ma to get minimum avg steps
    ohlcv_df['ma'] = ohlcv_df['close'].rolling(window=math.trunc(sub_interval)).mean()
    
    # round tma to reach window steps
    ohlcv_df['signal'] = ohlcv_df['ma'].rolling(window=int(np.round(sub_interval))).mean()
    
    ohlcv_df = ohlcv_df.drop(columns=['ma'])
    ohlcv_df = ohlcv_df.dropna().reset_index(drop=True)
    
    return ohlcv_df


def get_action(ohlcv_df, config_params):
    func_dict = {'tma': signal_tma}
    ohlcv_df = func_dict[config_params['signal']](ohlcv_df, config_params['window'])

    last_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']
    last_signal = ohlcv_df.loc[len(ohlcv_df) - 1, 'signal']

    if last_price < last_signal:
        action = 'sell'
    elif last_price > last_signal:
        action = 'buy'
    else:
        action = 'hold'
    
    return action


def get_current_position(exchange, config_params):
    positions = exchange.fetch_positions()
    indexed = exchange.index_by(positions, 'future')
    position = exchange.safe_value(indexed, config_params['symbol'])

    return position


def cal_new_amount(value, last_price, config_params):
    value *= config_params['safety_value']
    leverage_value = value * config_params['leverage']
    
    amount = leverage_value / last_price
    amount = round_down_amount(amount, config_params)
    
    return amount


def cal_reduce_amount(value, last_price, config_params):
    leverage_value = value * config_params['leverage']
    
    amount = leverage_value / last_price
    amount = round_up_amount(amount, config_params)

    return amount


def open_position(action, exchange, config_params, open_orders_df_path):
    last_price = get_last_price(exchange, config_params, print_flag=False)
    _, cash = get_balance(last_price, exchange, config_params)

    last_price = get_last_price(exchange, config_params, print_flag=False)
    amount = cal_new_amount(cash, last_price , config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order('amount', order, exchange, config_params, open_orders_df_path)

    return order


def close_position(action, position, exchange, config_params, open_orders_df_path):
    amount = position['size']
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order('amount', order, exchange, config_params, open_orders_df_path)

    return order


def reduce_position(value, action, exchange, config_params, open_orders_df_path):
    last_price = get_last_price(exchange, config_params, print_flag=False)
    amount = cal_reduce_amount(value, last_price, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order('amount', order, exchange, config_params, open_orders_df_path)

    return order


def clear_orders_technical(order, exchange, bot_name, base_currency, quote_currency, config_system, config_params, open_orders_df_path, transactions_df_path):
    order_id = order['id']
    
    while order['status'] != 'closed':
        order = exchange.fetch_order(order_id, config_params['symbol'])
        time.sleep(config_system['idle_stage'])

    remove_order(order_id, open_orders_df_path)
    append_order('filled', order, exchange, config_params, transactions_df_path)
    noti_success_order(order, bot_name, base_currency, quote_currency)


def append_profit_technical(amount, order, position, profit_df_path):
    df = pd.read_csv(profit_df_path)
    
    timestamp = get_time()
    close_id = order['id']
    open_price = position['entryPrice']
    close_price = order['price']

    if position['side'] == 'buy':
        margin = close_price - open_price
    elif position['side'] == 'sell':
        margin = open_price - close_price

    profit = margin * amount

    df.loc[len(df)] = [timestamp, close_id, position['symbol'], position['side'], amount, open_price, close_price, profit]
    df.to_csv(profit_df_path, index=False)


def update_budget_technical(last_price, prev_date, position, exchange, bot_name, config_params, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transactions_df = pd.read_csv(transactions_df_path)
    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    if (len(last_transactions_df) > 0) | (len(cash_flow_df) > 0):
        balance, cash = get_balance(last_price, exchange, config_params)

        transfer = get_transfer(transfer_path)

        cash_flow_list = [prev_date, balance, cash, position['realizedPnl'], transfer['deposit'], transfer['withdraw']]
        append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)

        transfer = get_transfer(transfer_path)
        withdraw_value = transfer['withdraw'] - transfer['deposit']
        reset_transfer(transfer_path)

    return withdraw_value