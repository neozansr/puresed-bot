import numpy as np
import pandas as pd
import datetime as dt
from dateutil import tz
from dateutil.relativedelta import relativedelta
import math
import time

from func_get import get_json, get_time, convert_tz, get_currency_future, get_last_price, get_quote_currency_free, get_quote_currency_value, get_order_fee, get_position_api
from func_cal import round_down_amount, round_up_amount, cal_unrealised_future, cal_drawdown_future, cal_available_budget, cal_end_balance
from func_update import update_json, append_order, remove_order, append_cash_flow_df, update_transfer
from func_noti import noti_success_order, noti_warning, print_position


def get_date_list(start_date, end_date=None):
    '''
    Generate list of date to fetch 1 day iteration from fetch_ohlcv.
    '''
    if end_date == None:
        end_date = dt.date.today()
    
    num_day = (end_date - start_date).days
    date_list = [end_date - relativedelta(days=x) for x in range(num_day, -1, -1)]
    
    return date_list

    
def get_js_date(dt_date, start_hour):
    '''
    Transform dt.datetime to JavaScript format.
    Result based on local timezone.
    '''
    dt_datetime = dt.datetime(dt_date.year, dt_date.month, dt_date.day, start_hour)
    js_datetime = dt_datetime.timestamp() * 1000
    
    return js_datetime


def get_base_time(date, base_timezone):
    '''
    Convert to base_timezone to check day light saving.
    DST change at 2 am.
    '''
    base_time = dt.datetime(date.year, date.month, date.day, 2, tzinfo=tz.gettz(base_timezone))
    
    return base_time


def cal_dst(date, base_timezone):
    today_dst = bool(get_base_time(date, base_timezone).dst())
    tomorrow_dst = bool(get_base_time(date + relativedelta(days=1), base_timezone).dst())
    
    return today_dst, tomorrow_dst


def cal_dst_offset(today_dst, tomorrow_dst):
    '''
    Offset ending of the day before DST change to be in sync with start time of the changing day.
    '''
    if (today_dst == 0) & (tomorrow_dst == 1):
        dst_offset = -60
    elif (today_dst == 1) & (tomorrow_dst == 0):
        dst_offset = 60
    else:
        dst_offset = 0
        
    return dst_offset


def get_start_hour(tomorrow_dst):
    if tomorrow_dst == 1:
        start_hour = 4
    else:
        start_hour = 5
        
    return start_hour


def get_timeframe(interval):
    timeframe_dict = {1440:'1d', 240:'4h', 60:'1h', 15:'15m', 5:'5m', 1:'1m'}
    
    for i in timeframe_dict.keys():
        if interval % i == 0:
            base_interval = i
            break
            
    base_timeframe = timeframe_dict[base_interval]
    step = int(interval / base_interval)
            
    return base_timeframe, base_interval, step


def group_timeframe(df, step):
    h_dict = {'time':[], 'open':[], 'high':[], 'low':[], 'close':[]}
            
    for i in [x for x in range(0, len(df), step)]:
        temp_df = df.iloc[i:min(i + step, len(df)), :].reset_index(drop=True)
        h_dict['time'].append(temp_df['time'][0])
        h_dict['open'].append(temp_df['open'][0])
        h_dict['high'].append(max(temp_df['high']))
        h_dict['low'].append(min(temp_df['low']))
        h_dict['close'].append(temp_df['close'][len(temp_df) - 1])

    df = pd.DataFrame(h_dict)
    
    return df

    
def get_ohlcv(exchange, config_params_path):
    ohlcv_df = pd.DataFrame(columns = ['time', 'open', 'high', 'low', 'close'])
    config_params = get_json(config_params_path)
    base_timeframe, base_interval, step = get_timeframe(config_params['interval'])

    # Get start date to cover window range.
    min_num_date = np.ceil((config_params['interval'] * config_params['window']) / 1440)
    start_date = dt.date.today() - relativedelta(days=min_num_date)
    date_list = get_date_list(start_date)

    for date in date_list:
        today_dst, tomorrow_dst = cal_dst(date, config_params['base_timezone'])
        dst_offset = cal_dst_offset(today_dst, tomorrow_dst)
        start_hour = get_start_hour(today_dst)
        limit = int((1440 / base_interval) + (dst_offset / base_interval))
        
        since = get_js_date(date, start_hour)
        ohlcv = exchange.fetch_ohlcv(config_params['symbol'], base_timeframe, since, limit)
        
        if len(ohlcv) > 0:
            temp_df = pd.DataFrame(ohlcv)
            temp_df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
            temp_df['time'] = pd.to_datetime(temp_df['time'], unit='ms')
            temp_df['time'] = temp_df['time'].apply(lambda x: convert_tz(x))
            
            # Remove timezone after offset timezone.
            temp_df['time'] = temp_df['time'].dt.tz_localize(None)
            
            if step > 1:
                date_df = group_timeframe(temp_df, step)
            else:
                date_df = temp_df[['time', 'open', 'high', 'low', 'close']]

            ohlcv_df = pd.concat([ohlcv_df, date_df])

    ohlcv_df = ohlcv_df.reset_index(drop=True)
    
    signal_timestamp = ohlcv_df['time'][len(ohlcv_df) - 1]
    ohlcv_df = ohlcv_df.iloc[:len(ohlcv_df) - 1, :]

    return ohlcv_df, signal_timestamp


def update_price(close_price, signal_price, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['close_price'] = float(close_price)
    last_loop['signal_price'] = signal_price

    update_json(last_loop, last_loop_path)


def update_side(side, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['side'] = side

    update_json(last_loop, last_loop_path)


def update_signal_timestamp(signal_timestamp, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['signal_timestamp'] = str(signal_timestamp)
    
    update_json(last_loop, last_loop_path)


def update_open_position(order, exchange, config_params, position_path):
    position = get_json(position_path)
    fee = get_order_fee('future', order, exchange, config_params)

    position['side'] = order['side']
    position['entry_price'] = order['price']
    position['amount'] = order['amount']
    position['open_fee'] = fee

    update_json(position, position_path)


def update_reduce_position(order, position_path):
    position = get_json(position_path)
    amount = position['amount']
    amount -= order['amount']

    position['amount'] = amount

    update_json(position, position_path)


def update_max_drawdown(drawdown, last_loop_path):
    last_loop = get_json(last_loop_path)
    last_loop['max_drawdown'] = drawdown

    update_json(last_loop, last_loop_path)


def check_new_timestamp(signal_timestamp, config_params_path, last_loop_path):
    config_params = get_json(config_params_path)
    last_loop = get_json(last_loop_path)

    if last_loop['signal_timestamp'] == 0:
        # One first loop, bypass to manage_position to update last_loop.
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
    
    # Trunc ma to get minimum avg steps.
    ohlcv_df['ma'] = ohlcv_df['close'].rolling(window=math.trunc(sub_interval)).mean()
    
    # Round tma to reach window steps.
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


def append_profit_technical(order, exchange, config_params, position_path, profit_df_path):
    profit_df = pd.read_csv(profit_df_path)
    position = get_json(position_path)
    
    timestamp = get_time()

    if position['side'] == 'buy':
        margin = order['price'] - position['entry_price']
    elif position['side'] == 'sell':
        margin = position['entry_price'] - order['price']

    open_fee = position['open_fee']
    close_fee = get_order_fee('future', order, exchange, config_params)
    profit = (margin * order['amount']) - (open_fee + close_fee)

    profit_df.loc[len(profit_df)] = [timestamp, order['id'], order['symbol'], order['side'], order['amount'], position['entry_price'], order['price'], profit]
    profit_df.to_csv(profit_df_path, index=False)


def open_position(available_budget, action, exchange, config_params_path, open_orders_df_path):
    config_params = get_json(config_params_path)
    amount = cal_new_amount(available_budget, exchange, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order(order, 'amount', open_orders_df_path)


def close_position(action, position, exchange, config_params_path, open_orders_df_path):
    config_params = get_json(config_params_path)
    order = exchange.create_order(config_params['symbol'], 'market', action, position['amount'], params={'reduceOnly': True})
    
    append_order(order, 'amount', open_orders_df_path)


def reduce_position(value, action, exchange, config_params_path, open_orders_df_path):
    config_params = get_json(config_params_path)
    amount = cal_reduce_amount(value, exchange, config_params)
    order = exchange.create_order(config_params['symbol'], 'market', action, amount)
    
    append_order(order, 'amount', open_orders_df_path)


def clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)

    if len(open_orders_df) > 0:
        order_id = open_orders_df['order_id'][0]
        order = exchange.fetch_order(order_id, config_params['symbol'])

        while order['status'] != 'closed':
            order = exchange.fetch_order(order_id, config_params['symbol'])
            time.sleep(config_system['idle_stage'])

        remove_order(order_id, open_orders_df_path)
        append_order(order, 'filled', transactions_df_path)
        noti_success_order(order, bot_name, config_params, future=True)

    return order


def withdraw_position(net_transfer, exchange, bot_name, config_system, config_params_path, position_path, open_orders_df_path, transactions_df_path, profit_df_path):
    config_params = get_json(config_params_path)
    position = get_json(position_path)
    
    reverse_action = {'buy':'sell', 'sell':'buy'}
    action = reverse_action[position['side']]
    reduce_position(-net_transfer, action, exchange, config_params_path, open_orders_df_path)

    time.sleep(config_system['idle_stage'])
    
    reduce_order = clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
    append_profit_technical(reduce_order, exchange, config_params, position_path, profit_df_path)
    update_reduce_position(reduce_order, position_path)


def manage_position(ohlcv_df, exchange, bot_name, config_system, config_params_path, last_loop_path, position_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    config_params = get_json(config_params_path)
    _, quote_currency = get_currency_future(config_params)
    last_loop = get_json(last_loop_path)
    action, signal_price = get_action(ohlcv_df, config_params)
    close_price = ohlcv_df.loc[len(ohlcv_df) - 1, 'close']

    print(f"Close price: {close_price:.2f} {quote_currency}")
    print(f"Signal price: {signal_price:.2f} {quote_currency}")

    if action == 'hold':
        action = last_loop['side']

    if last_loop['side'] not in [action, 'start']:
        position = get_json(position_path)

        if position['amount'] > 0:
            close_position(action, position, exchange, config_params_path, open_orders_df_path)
            time.sleep(config_system['idle_stage'])

            close_order = clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
            append_profit_technical(close_order, exchange, config_params, position_path, profit_df_path)
            update_reduce_position(close_order, position_path)
        
        transfer = get_json(transfer_path)

        # Technical bot not collect cash flow.
        available_cash_flow = 0
        quote_currency_free = get_quote_currency_free(exchange, quote_currency)
        available_budget = cal_available_budget(quote_currency_free, available_cash_flow, transfer)
        open_position(available_budget, action, exchange, config_params_path, open_orders_df_path)
        time.sleep(config_system['idle_stage'])

        open_order = clear_orders_technical(exchange, bot_name, config_system, config_params, open_orders_df_path, transactions_df_path)
        update_open_position(open_order, exchange, config_params, position_path)

    else:
        print("No action")
    
    update_price(close_price, signal_price, last_loop_path)
    update_side(action, last_loop_path)


def update_end_date_technical(prev_date, exchange, bot_name, config_system, config_params_path, position_path, transfer_path, open_orders_df_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    config_params = get_json(config_params_path)
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    
    last_price = get_last_price(exchange, config_params)
    position = get_json(position_path)

    _, quote_currency = get_currency_future(config_params)

    unrealised = cal_unrealised_future(last_price, position)

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    profit = sum(last_profit_df['profit'])

    transfer = get_json(transfer_path)
    net_transfer = transfer['deposit'] - transfer['withdraw']

    if net_transfer < 0:
        withdraw_position(net_transfer, exchange, bot_name, config_system, config_params_path, position_path, open_orders_df_path, transactions_df_path, profit_df_path)

    # Techical port don't hold actual asset.
    current_value = 0
    cash = get_quote_currency_value(exchange, quote_currency)
    end_balance = cal_end_balance(current_value, cash, transfer)

    cash_flow_list = [
        prev_date,
        end_balance,
        unrealised,
        profit,
        transfer['deposit'],
        transfer['withdraw']
        ]

    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_transfer(config_params, transfer_path)


def check_drawdown(exchange, bot_name, config_params_path, last_loop_path, position_path):
    config_params = get_json(config_params_path)
    position = get_json(position_path)
    
    if position['amount'] > 0:
        last_loop = get_json(last_loop_path)
        last_price = get_last_price(exchange, config_params)

        drawdown = cal_drawdown_future(last_price, position)
        
        if drawdown > last_loop['max_drawdown']:
            noti_warning(f"Drawdown {drawdown * 100:.2f}%", bot_name)
            update_max_drawdown(drawdown, last_loop_path)


def print_report_technical(exchange, config_params_path, position_path):
    config_params = get_json(config_params_path)
    _, quote_currency = get_currency_future(config_params)
    last_price = get_last_price(exchange, config_params)
    position = get_json(position_path)

    if position['amount'] > 0:
        position_api = get_position_api(exchange, config_params)
        print_position(last_price, position, position_api, quote_currency)