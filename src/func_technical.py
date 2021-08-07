import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import math
import time
import json

from func_get import get_json, get_time, convert_tz, get_currency_future, get_last_price, get_quote_currency_value, get_current_position
from func_cal import round_down_amount, round_up_amount
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


def update_max_drawdown(drawdown, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['max_drawdown'] = drawdown

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def check_new_timestamp(ohlcv_df, config_params, last_loop):
    signal_timestamp = ohlcv_df.loc[len(ohlcv_df) - 1, 'time']
    print(f'Signal timestamp: {signal_timestamp}')

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
    
    return amount, last_price


def cal_reduce_amount(value, exchange, config_params):
    leverage_value = value * config_params['leverage']
    
    last_price = get_last_price(exchange, config_params)
    amount = leverage_value / last_price
    amount = round_up_amount(amount, config_params)

    return amount, last_price


def open_position(action, exchange, config_params, open_orders_df_path):
    base_currency, quote_currency = get_currency_future(config_params)
    balance_value = get_quote_currency_value(exchange, quote_currency)

    amount, last_price = cal_new_amount(balance_value, exchange, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order('amount', order, exchange, config_params, open_orders_df_path)
    print(f'Open {action} {amount:.3f} {base_currency} at {last_price} {quote_currency}')

    return order


def close_position(action, position, exchange, config_params, open_orders_df_path):
    base_currency, quote_currency = get_currency_future(config_params)
    amount = position['size']
    order = exchange.create_order(config_params['symbol'], 'market', action, amount, params={'reduceOnly': True})
    last_price = get_last_price(exchange, config_params)
    
    append_order('amount', order, exchange, config_params, open_orders_df_path)
    # size from future dict is str type
    print(f'Open {action} {amount} {base_currency} at {last_price} {quote_currency}')

    return order


def reduce_position(value, action, exchange, config_params, open_orders_df_path):
    base_currency, quote_currency = get_currency_future(config_params)
    
    amount, last_price = cal_reduce_amount(value, exchange, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order('amount', order, exchange, config_params, open_orders_df_path)
    print(f'Open {action} {amount:.3f} {base_currency} at {last_price} {quote_currency}')

    return order


def clear_orders_technical(order, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path):
    order_id = order['id']
    
    while order['status'] != 'closed':
        order = exchange.fetch_order(order_id, config_params['symbol'])
        time.sleep(config_system['idle_stage'])

    remove_order(order_id, open_orders_df_path)
    append_order('filled', order, exchange, config_params, transactions_df_path)
    noti_success_order(order, bot_name, config_params, future=True)


def withdraw_position(prev_date, exchange, bot_name, config_system, config_params, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    position = get_current_position(exchange, config_params)
    withdraw_value = update_budget_technical(prev_date, position, exchange, bot_name, config_params, transfer_path, cash_flow_df_path)
    
    if withdraw_value > 0:
        reverse_action = {'buy':'sell', 'sell':'buy'}
        action = reverse_action[position['side']]
        reduce_order = reduce_position(withdraw_value, action, exchange, config_params, open_orders_df_path)

        time.sleep(config_system['idle_stage'])
        
        clear_orders_technical(reduce_order, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
        append_profit_technical(reduce_order['amount'], reduce_order, position, profit_df_path)


def manage_position(ohlcv_df, exchange, bot_name, config_system, config_params, last_loop_path, open_orders_df_path, transactions_df_path, profit_df_path):
    _, quote_currency = get_currency_future(config_params)
    last_loop = get_json(last_loop_path)
    action, signal_price = get_action(ohlcv_df, config_params)
    close_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']

    print(f'Close price: {close_price:.2f} {quote_currency}')
    print(f'Signal price: {signal_price:.2f} {quote_currency}')

    if action == 'hold':
        action = last_loop['side']

    if last_loop['side'] not in [action, 'start']:
        position = get_current_position(exchange, config_params)

        if position != None:
            if position['size'] != '0.0':
                close_order = close_position(action, position, exchange, config_params, open_orders_df_path)
                time.sleep(config_system['idle_stage'])

                clear_orders_technical(close_order, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
                append_profit_technical(close_order['amount'], close_order, position, profit_df_path)

        open_order = open_position(action, exchange, config_params, open_orders_df_path)
        time.sleep(config_system['idle_stage'])

        clear_orders_technical(open_order, exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)

    else:
        print('No action')
    
    update_ohlcv(ohlcv_df, last_loop_path)
    update_signal_price(signal_price, last_loop_path)
    update_side(action, last_loop_path)


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


def update_budget_technical(prev_date, position, exchange, bot_name, config_params, transfer_path, cash_flow_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)

    if position != None:
        if position['size'] != '0.0':
            realised = position['realizedPnl']
        else:
            realised = 0
    else:
        realised = 0

    _, quote_currency = get_currency_future(config_params)
    balance_value = get_quote_currency_value(exchange, quote_currency)

    transfer = get_json(transfer_path)

    cash_flow_list = [prev_date, balance_value, realised, transfer['deposit'], transfer['withdraw']]
    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)

    transfer = get_json(transfer_path)
    withdraw_value = transfer['withdraw'] - transfer['deposit']
    reset_transfer(transfer_path)

    return withdraw_value


def check_drawdown(position, exchange, config_params, last_loop, bot_name, last_loop_path):
    last_price = get_last_price(exchange, config_params)
    entry_price = float(position['entryPrice'])

    if position['side'] == 'buy':
        drawdown = max(1 - (last_price / entry_price), 0)
    elif position['side'] == 'sell':
        drawdown = max((last_price / entry_price) - 1, 0)
    
    if drawdown > last_loop['max_drawdown']:
        noti_warning(f'Drawdown {drawdown * 100:.2f}%', bot_name)
        update_max_drawdown(drawdown, last_loop_path)


def print_report_technical(position, exchange, config_params):
    _, quote_currency = get_currency_future(config_params)
    last_price = get_last_price(exchange, config_params)

    print_position(last_price, position, quote_currency)