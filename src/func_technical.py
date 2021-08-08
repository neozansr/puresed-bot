import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import math
import time
import json

from func_get import get_json, get_time, convert_tz, get_currency_future, get_last_price, get_quote_currency_value, get_position_api
from func_cal import round_down_amount, round_up_amount, cal_unrealised_future, cal_drawdown_future
from func_update import append_order, remove_order, append_cash_flow_df, reset_transfer
from func_noti import noti_success_order, noti_warning, print_position


def get_timeframe(config_params):
    timeframe_dict = {1440:'1d', 240:'4h', 60:'1h', 30:'30m', 15:'15m'}
    
    for i in timeframe_dict.keys():
        if config_params['interval'] % i == 0:
            min_interval = i
            break
            
    min_timeframe = timeframe_dict[min_interval]
    step = int(config_params['interval'] / min_interval)
            
    return min_timeframe, step


def group_timeframe(ohlcv_df, step):
    ohlcv_dict = {'time':[], 'open':[], 'high':[], 'low':[], 'close':[]}
    
    mod = len(ohlcv_df) % step
    if  mod != 0:
        ohlcv_df = ohlcv_df.iloc[:-mod, :]
        
    for i in [x for x in range(0, len(ohlcv_df), step)]:
        temp_df = ohlcv_df.iloc[i:i + step, :]
        ohlcv_dict['time'].append(temp_df['time'][i])
        ohlcv_dict['open'].append(temp_df['open'][i])
        ohlcv_dict['high'].append(max(temp_df['high']))
        ohlcv_dict['low'].append(min(temp_df['low']))
        ohlcv_dict['close'].append(temp_df['close'][i + 1])

    ohlcv_df = pd.DataFrame(ohlcv_dict)
    
    return ohlcv_df


def get_ohlcv(exchange, config_params):
    min_timeframe, step = get_timeframe(config_params)

    ohlcv = exchange.fetch_ohlcv(config_params['symbol'], timeframe=min_timeframe, limit=config_params['window'] * step)
    ohlcv_df = pd.DataFrame(ohlcv)
    ohlcv_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    ohlcv_df['time'] = pd.to_datetime(ohlcv_df['time'], unit='ms')
    ohlcv_df['time'] = ohlcv_df['time'].apply(lambda x: convert_tz(x))

    if step > 1:
        ohlcv_df = group_timeframe(ohlcv_df, step)

    return ohlcv_df


def update_ohlcv(ohlcv_df, last_loop_path):
    signal_timestamp = ohlcv_df.loc[len(ohlcv_df) - 1, 'time']
    close_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']

    last_loop = get_json(last_loop_path)
    last_loop['signal_timestamp'] = str(signal_timestamp)
    last_loop['close_price'] = float(close_price)

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_signal_price(signal_price, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['signal_price'] = signal_price

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_side(side, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['side'] = side

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_open_position(order, position_path):
    position = get_json(position_path)
    position['side'] = order['side']
    position['entry_price'] = order['price']
    position['amount'] = order['amount']

    with open(position_path, 'w') as position_file:
        json.dump(position, position_file, indent=1)


def update_reduce_position(order, position_path):
    position = get_json(position_path)
    amount = position['amount']
    amount -= order['amount']

    position['amount'] = amount

    with open(position_path, 'w') as position_file:
        json.dump(position, position_file, indent=1)


def update_max_drawdown(drawdown, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['max_drawdown'] = drawdown

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def check_new_timestamp(ohlcv_df, config_params, last_loop_path):
    signal_timestamp = ohlcv_df.loc[len(ohlcv_df) - 1, 'time']
    print(f'Signal timestamp: {signal_timestamp}')

    last_loop = get_json(last_loop_path)

    if last_loop['signal_timestamp'] == 0:
        # One first loop, bypass to manage_position to update last_loop
        new_timestamp_flag = True
    else:
        last_signal_timestamp = pd.to_datetime(last_loop['signal_timestamp'])
        expected_timestamp = last_signal_timestamp + relativedelta(minutes=config_params['interval'])
        
        if signal_timestamp >= expected_timestamp:
            new_timestamp_flag = True
        else:
            new_timestamp_flag = False

    return new_timestamp_flag


def action_cross_signal(ohlcv_df):
    close_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']
    signal_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'signal']

    if close_price < signal_price:
        action = 'sell'
    elif close_price > signal_price:
        action = 'buy'
    else:
        action = 'hold'

    return action, signal_price


def action_bound_signal(ohlcv_df):
    close_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']
    last_min_signal = ohlcv_df.loc[len(ohlcv_df) - 1, 'min_signal']
    last_max_signal = ohlcv_df.loc[len(ohlcv_df) - 1, 'max_signal']
    signal_price = {'min':last_min_signal, 'max':last_max_signal}

    if close_price < last_min_signal:
        action = 'buy'
    elif close_price > last_max_signal:
        action = 'sell'
    else:
        action = 'hold'

    return action, signal_price


def signal_ma(ohlcv_df, config_params):
    ohlcv_df['signal'] = ohlcv_df['close'].rolling(window=int(np.round(config_params['window']))).mean()
    
    action, signal_price = action_cross_signal(ohlcv_df)
    
    return action, signal_price


def signal_tma(ohlcv_df, config_params):
    sub_interval = (config_params['window'] + 1) / 2
    
    # trunc ma to get minimum avg steps
    ohlcv_df['ma'] = ohlcv_df['close'].rolling(window=math.trunc(sub_interval)).mean()
    
    # round tma to reach window steps
    ohlcv_df['signal'] = ohlcv_df['ma'].rolling(window=int(np.round(sub_interval))).mean()

    action, signal_price = action_cross_signal(ohlcv_df)
    
    return action, signal_price


def signal_bollinger(ohlcv_df, config_params):
    ohlcv_df['ma'] = ohlcv_df['close'].rolling(window=int(np.round(config_params['window']))).mean()
    ohlcv_df['std'] = ohlcv_df['close'].rolling(window=int(np.round(config_params['window']))).std()

    ohlcv_df['min_signal'] = ohlcv_df['ma'] - (2 * ohlcv_df['std'])
    ohlcv_df['max_signal'] = ohlcv_df['ma'] + (2 * ohlcv_df['std'])
    
    action, signal_price = action_bound_signal(ohlcv_df)
    
    return action, signal_price


def get_action(ohlcv_df, config_params):
    func_dict = {
        'ma':signal_ma, 
        'tma': signal_tma,
        'bollinger': signal_bollinger
        }
    action, signal_price = func_dict[config_params['signal']](ohlcv_df, config_params)
    
    return action, signal_price


def cal_new_amount(value, exchange, config_params):
    value *= config_params['safety_value']
    leverage_value = value * config_params['leverage']
    
    last_price = get_last_price(exchange, config_params)
    amount = leverage_value / last_price
    amount = round_down_amount(amount, config_params)
    
    return amount


def cal_reduce_amount(value, exchange, config_params):
    leverage_value = value * config_params['leverage']
    
    last_price = get_last_price(exchange, config_params)
    amount = leverage_value / last_price
    amount = round_up_amount(amount, config_params)

    return amount


def append_profit_technical(amount, order, position, last_loop_path, profit_df_path):
    profit_df = pd.read_csv(profit_df_path)
    last_loop = get_json(last_loop_path)
    
    timestamp = get_time()
    close_id = order['id']
    open_price = last_loop['entry_price']
    close_price = order['price']

    if position['side'] == 'buy':
        margin = close_price - open_price
    elif position['side'] == 'sell':
        margin = open_price - close_price

    profit = margin * amount

    profit_df.loc[len(profit_df)] = [timestamp, close_id, position['symbol'], position['side'], amount, open_price, close_price, profit]
    profit_df.to_csv(profit_df_path, index=False)


def open_position(action, exchange, config_params, open_orders_df_path):
    _, quote_currency = get_currency_future(config_params)
    balance_value = get_quote_currency_value(exchange, quote_currency)

    amount = cal_new_amount(balance_value, exchange, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order(order, 'amount', config_params, open_orders_df_path)


def close_position(action, position, exchange, config_params, open_orders_df_path):
    amount = position['size']
    order = exchange.create_order(config_params['symbol'], 'market', action, amount, params={'reduceOnly': True})
    
    append_order(order, 'amount', config_params, open_orders_df_path)


def reduce_position(value, action, exchange, config_params, open_orders_df_path):
    amount = cal_reduce_amount(value, exchange, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order(order, 'amount', config_params, open_orders_df_path)


def clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, config_params['symbol'])

        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, config_params['symbol'])
            time.sleep(config_system['idle_stage'])

        remove_order(order_id, open_orders_df_path)
        append_order(order, 'filled', config_params, transactions_df_path)
        noti_success_order(order, bot_name, config_params, future=True)

    return order


def withdraw_position(withdraw_value, exchange, bot_name, config_system, config_params, last_loop_path, position_path, open_orders_df_path, transactions_df_path, profit_df_path):
    last_loop = get_json(last_loop_path)
    position = get_json(position_path)
    
    reverse_action = {'buy':'sell', 'sell':'buy'}
    action = reverse_action[position['side']]
    reduce_position(withdraw_value, action, exchange, config_params, open_orders_df_path)

    time.sleep(config_system['idle_stage'])
    
    reduce_order = clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
    append_profit_technical(reduce_order['amount'], reduce_order, position, last_loop, profit_df_path)
    update_reduce_position(reduce_order, position_path)


def manage_position(ohlcv_df, exchange, bot_name, config_system, config_params, last_loop_path, position_path, open_orders_df_path, transactions_df_path, profit_df_path):
    _, quote_currency = get_currency_future(config_params)
    last_loop = get_json(last_loop_path)
    action, signal_price = get_action(ohlcv_df, config_params)
    close_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']

    print(f'Close price: {close_price:.2f} {quote_currency}')
    print(f'Signal price: {signal_price:.2f} {quote_currency}')

    if action == 'hold':
        action = last_loop['side']

    if last_loop['side'] not in [action, 'start']:
        position = get_json(position_path)

        if position['amount'] > 0:
            close_position(action, position, exchange, config_params, open_orders_df_path)
            time.sleep(config_system['idle_stage'])

            close_order = clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
            append_profit_technical(close_order['amount'], close_order, position, last_loop, profit_df_path)
            update_reduce_position(close_order, position_path)

        open_position(action, exchange, config_params, open_orders_df_path)
        time.sleep(config_system['idle_stage'])

        open_order = clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
        update_open_position(open_order, position_path)

    else:
        print("No action")
    
    update_ohlcv(ohlcv_df, last_loop_path)
    update_signal_price(signal_price, last_loop_path)
    update_side(action, last_loop_path)


def update_budget_technical(prev_date, exchange, bot_name, config_params, position_path, transfer_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    last_price = get_last_price(exchange, config_params)
    position = get_json(position_path)

    _, quote_currency = get_currency_future(config_params)
    balance_value = get_quote_currency_value(exchange, quote_currency)

    unrealised = cal_unrealised_future(last_price, position)
    transfer = get_json(transfer_path)

    cash_flow_list = [prev_date, balance_value, unrealised, transfer['deposit'], transfer['withdraw']]
    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)

    transfer = get_json(transfer_path)
    withdraw_value = transfer['withdraw'] - transfer['deposit']
    reset_transfer(transfer_path)

    return withdraw_value


def check_drawdown(exchange, bot_name, config_params, last_loop_path, position_path):
    position = get_json(position_path)
    
    if position['amount'] > 0:
        last_loop = get_json(last_loop_path)
        last_price = get_last_price(exchange, config_params)

        drawdown = cal_drawdown_future(last_price, position)
        
        if drawdown > last_loop['max_drawdown']:
            noti_warning(f"Drawdown {drawdown * 100:.2f}%", bot_name)
            update_max_drawdown(drawdown, last_loop_path)


def print_report_technical(exchange, config_params, position_path):
    _, quote_currency = get_currency_future(config_params)
    last_price = get_last_price(exchange, config_params)
    
    position = get_json(position_path)
    position_api = get_position_api(exchange, config_params)

    print_position(last_price, position, position_api, quote_currency)