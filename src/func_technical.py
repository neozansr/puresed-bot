import numpy as np
import pandas as pd
import datetime as dt
import time

import func_get
import func_cal
import func_signal
import func_update


def floor_dt(timestamp, round_minute):
    '''
    Round timestamp to previous minute interval.
    '''
    delta = dt.timedelta(minutes=round_minute)
    round_timestamp = timestamp - (timestamp - dt.datetime.min) % delta

    return round_timestamp


def get_fetch_timeframe(action_timeframe):
    exchange_interval_dict = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '1h': 60,
        '4h': 240,
        '1d': 1440
    }

    fetch_interval_dict = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '30m': 30,
        '1h': 60,
        '2h': 120,
        '4h': 240,
        '1d': 1440
    }

    inverse_exchange_interval_dict = {v: k for k, v in exchange_interval_dict.items()}

    exchange_interval_list = list(inverse_exchange_interval_dict)
    exchange_interval_list.sort(reverse=True)

    for fetch_interval in exchange_interval_list:
        if fetch_interval_dict[action_timeframe] % fetch_interval == 0:
            break

    fetch_timeframe = inverse_exchange_interval_dict[fetch_interval]
    step = int(fetch_interval_dict[action_timeframe] / fetch_interval)
    
    return fetch_timeframe, step

    
def get_ohlcv_df(exchange, symbol, timeframe, limit):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)[:-1]

    if len(ohlcv) > 0:
        ohlcv_df = pd.DataFrame(ohlcv)
        ohlcv_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
        ohlcv_df['time'] = pd.to_datetime(ohlcv_df['time'], unit='ms')
        ohlcv_df['time'] = ohlcv_df['time'].apply(lambda x: func_get.convert_tz(x))

        # Remove timezone after offset timezone
        ohlcv_df['time'] = ohlcv_df['time'].dt.tz_localize(None)
    else:
        ohlcv_df = pd.DataFrame()
        
    return ohlcv_df


def group_timeframe(ohlcv_df, step):
    ohlcv_dict = {'time':[], 'open':[], 'high':[], 'low':[], 'close':[]}
            
    for i in [x for x in range(0, len(ohlcv_df), step)]:
        temp_df = ohlcv_df.iloc[i:min(i + step, len(ohlcv_df)), :].reset_index(drop=True)
        ohlcv_dict['time'].append(temp_df['time'][0])
        ohlcv_dict['open'].append(temp_df['open'][0])
        ohlcv_dict['high'].append(max(temp_df['high']))
        ohlcv_dict['low'].append(min(temp_df['low']))
        ohlcv_dict['close'].append(temp_df['close'][len(temp_df) - 1])

    grouped_ohlcv_df = pd.DataFrame(ohlcv_dict)
    
    return grouped_ohlcv_df


def get_timeframe_list(symbol_type, config_params):
    open_timeframe_list = list(config_params[symbol_type]['open'])
    close_timeframe_list = list(config_params[symbol_type]['close'])
    timeframe_list = open_timeframe_list + close_timeframe_list

    for stop_key in ['tp', 'sl']:
        if (symbol_type == 'base') & (config_params[stop_key]['signal'] != None):
            stop_timeframe = config_params[stop_key]['signal']['timeframe']
            timeframe_list += [stop_timeframe]
        
    timeframe_list = list(set(timeframe_list))

    return timeframe_list


def get_ohlcv_df_dict(exchange, config_params):
    ohlcv_df_dict = {
        'base': {},
        'lead': {}
        }
    
    for symbol_type in ['base', 'lead']:
        symbol_list = config_params[symbol_type]['symbol']
        timeframe_list = get_timeframe_list(symbol_type, config_params)

        for timeframe in timeframe_list:
            ohlcv_df_dict[symbol_type][timeframe] = {}
            fetch_timeframe, step = get_fetch_timeframe(timeframe)
            
            for symbol in symbol_list:
                ohlcv_df = get_ohlcv_df(exchange, symbol, fetch_timeframe, config_params['safety_ohlcv_range'])
                
                if step > 1:
                    ohlcv_df = group_timeframe(ohlcv_df, step)
                    
                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df.reset_index(drop=True)

    return ohlcv_df_dict


def get_position_list(exchange):
    position_list = []

    position_dict = exchange.fetch_positions()
    position_dict = exchange.index_by(position_dict, 'future')

    for symbol in position_dict:
        if float(position_dict[symbol]['size']) > 0:
            position_list.append(symbol)

    return position_list


def get_open_symbol_list(exchange, config_params):
    position_list = get_position_list(exchange)
    max_position = int(100 / config_params['action_percent'])
    available_position = max_position - len(position_list)
    open_symbol_list = [x for x in config_params['base']['symbol'] if x not in position_list][:available_position]

    return open_symbol_list


def get_action_base(symbol, objective, action_list, config_params, ohlcv_df_dict):
    for timeframe in config_params['base'][objective]:
        ohlcv_df = ohlcv_df_dict['base'][timeframe][symbol]

        for signal in config_params['base'][objective][timeframe]:
            for func_name in config_params['base'][objective][timeframe][signal]['check']:
                action_side = func_signal.call_check_signal_func(func_name)(objective, 'base', signal, action_list, ohlcv_df, timeframe, config_params)
                print(f"     base {symbol} {func_name} {signal} {timeframe}: {action_side}")

    return action_list


def get_action_lead(objective, action_list, config_params, ohlcv_df_dict):
    for timeframe in config_params['lead'][objective]:
        for lead_symbol in config_params['lead']['symbol']:
            ohlcv_df = ohlcv_df_dict['lead'][timeframe][lead_symbol]

            for signal in config_params['lead'][objective][timeframe]:
                for func_name in config_params['lead'][objective][timeframe][signal]['check']:
                    action_side = func_signal.call_check_signal_func(func_name)(objective, 'lead', signal, action_list, ohlcv_df, timeframe, config_params)
                    print(f"     lead {lead_symbol} {func_name} {signal} {timeframe}: {action_side}")

    return action_list


def get_action(symbol, objective, action_list, config_params, ohlcv_df_dict):
    action_list = get_action_base(symbol, objective, action_list, config_params, ohlcv_df_dict)
    action_list = get_action_lead(objective, action_list, config_params, ohlcv_df_dict)    

    return action_list


def get_open_position_flag(symbol, config_params, ohlcv_df_dict):
    action_list = []
    action_list = get_action(symbol, 'open', action_list, config_params, ohlcv_df_dict)

    if (len(set(action_list)) == 1) & (action_list[0] in config_params['target_side']) & (action_list[0] != 'no_action'):
        open_position_flag = True
        side = action_list[0]
    else:
        open_position_flag = False
        side = None
        print(f"     No action")

    return open_position_flag, side


def get_stop_side(stop_key, side):
    if ((stop_key == 'tp') & (side == 'buy')) | ((stop_key == 'sl') &(side == 'sell')):
        stop_side = 'upper'
    elif ((stop_key == 'tp') & (side == 'sell')) | ((stop_key == 'sl') & (side == 'buy')):
        stop_side = 'lower'
    else:
        stop_side = None

    return stop_side


def get_stop_price_percent(stop_key, stop_side, open_price, stop_price_list, config_params):
    if config_params[stop_key]['price_percent'] != None:
        if stop_side == 'upper':
            price_percent_stop_price = open_price * (1 + (config_params[stop_key]['price_percent'] / 100))
        elif stop_side == 'lower':
            price_percent_stop_price = open_price * (1 - (config_params[stop_key]['price_percent'] / 100))
        else:
            price_percent_stop_price = None

        stop_price_list.append(price_percent_stop_price)

    return stop_price_list


def get_stop_price_signal(stop_key, symbol, stop_price_list, ohlcv_df_dict, config_params):
    if config_params[stop_key]['signal'] != None:
        ohlcv_df = ohlcv_df_dict['base'][config_params[stop_key]['signal']['timeframe']][symbol]
        check_series = ohlcv_df.loc[len(ohlcv_df) - 1, :]

        signal = list(config_params[stop_key]['signal']['signal'])[0]
        stop_price_list.append(check_series[signal])

    return stop_price_list


def get_stop_price(stop_key, side, symbol, open_price, ohlcv_df_dict, config_params):
    stop_price_list = []

    stop_side = get_stop_side(stop_key, side)
    stop_price_list = get_stop_price_percent(stop_key, stop_side, open_price, stop_price_list, config_params)
    stop_price_list = get_stop_price_signal(stop_key, symbol, stop_price_list, ohlcv_df_dict, config_params)

    if (stop_side == 'upper') & (len(stop_price_list) > 0):
        stop_price = min(stop_price_list)
    elif (stop_side == 'lower') & (len(stop_price_list) > 0):
        stop_price = max(stop_price_list)
    elif (stop_side == 'upper') & (len(stop_price_list) == 0):
        stop_price = np.inf
    elif (stop_side == 'lower') & (len(stop_price_list) == 0):
        stop_price = 0
    else:
        stop_price = None

    return stop_price


def send_stop_order(stop_key, order, exchange, config_params, ohlcv_df_dict):
    revert_dict = {
            'buy': 'sell',
            'sell': 'buy'
        }

    if stop_key == 'tp':
        stop_kw = 'takeProfit'
    elif stop_key == 'sl':
        stop_kw = 'stop'

    stop_amount = order['amount'] * config_params[stop_key]['stop_percent']
    stop_rounded_amount = func_cal.round_amount(stop_amount, exchange, order['symbol'], round_direction='down')
    stop_price = get_stop_price(stop_key, order['side'], order['symbol'], order['price'], ohlcv_df_dict, config_params)
    stop_order = exchange.createOrder(
        order['symbol'],
        stop_kw,
        revert_dict[order['side']],
        stop_rounded_amount,
        params={
            'triggerPrice': stop_price,
            'postOnly': True
            }
        )

    return stop_order


def open_position(exchange, symbol, config_system, config_params, ohlcv_df_dict, last_loop_path, transactions_df_path):
    open_position_flag, side = get_open_position_flag(symbol, config_params, ohlcv_df_dict)
    price = func_get.get_last_price(exchange, symbol)

    if open_position_flag:
        amount = ((config_params['action_percent'] / 100) * config_params['budget']) / price * config_params['leverage']
        rounded_amount = func_cal.round_amount(amount, exchange, symbol, round_direction='down')
        order = exchange.create_order(symbol, 'market', side, rounded_amount)

        order_id = order['id']
        order = exchange.fetch_order(order_id)

        while order['status'] != 'closed':
            time.sleep(config_system['idle_stage'])
            order = exchange.fetch_order(order_id)

        func_update.append_order(order, 'filled', 'open_order', transactions_df_path)

        if config_params['tp']['stop_percent'] > 0:
            tp_order = send_stop_order('tp', order, exchange, config_params, ohlcv_df_dict)
            tp_order_id = tp_order['id']
        else:
            tp_order_id = -1
        
        if config_params['sl']['stop_percent'] > 0:
            sl_order = send_stop_order('sl', order, exchange, config_params, ohlcv_df_dict)
            sl_order_id = sl_order['id']
        else:
            sl_order_id = -1

    last_loop = func_get.get_json(last_loop_path)
    last_loop['position'][symbol] = {
        'open_order_id': order_id,
        'tp_order_id': tp_order_id,
        'sl_order_id': sl_order_id
    }

    func_update.update_json(last_loop, last_loop_path)