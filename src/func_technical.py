import pandas as pd

from func_get import get_base_currency_value, get_json, get_quote_currency_value, get_time, get_order_fee, get_last_price
from func_cal import cal_adjusted_price, cal_end_balance, cal_unrealised_future
from func_update import append_order, append_cash_flow_df, update_transfer


def get_ohlcv():
    '''
    Fetch candle stick from exchange at the longest windows signal
    '''
    pass


def add_ema():
    '''
    Add ema_signal to ohlcv
    '''
    pass


def add_supertrend():
    '''
    Add supertrend to ohlcv
    '''
    pass


def add_wt_cross():
    '''
    Add wt_cross to ohlcv
    '''
    pass


def get_signal():
    get_ohlcv()
    add_ema()
    add_supertrend()
    add_wt_cross()


def append_profit_technical(exchange, order, last_loop_dict, profit_df_path):
    '''
    Cal profit from close order.
    Record profit on profit file.
    '''
    profit_df = pd.read_csv(profit_df_path)

    timestamp = get_time()
    order_id = order['id']    
    symbol = order['symbol']
    side = order['side']
    amount = order['amount']
    
    fee = get_order_fee(order, exchange, symbol)
    open_price = last_loop_dict[symbol]['open_price']
    close_price = cal_adjusted_price(order, fee, side)
    profit = (close_price - open_price) * amount       

    profit_df.loc[len(profit_df)] = [timestamp, order_id, symbol, side, amount, open_price, close_price, fee, profit]
    profit_df.to_csv(profit_df_path, index=False)


def cal_budget_technical():
    '''
    Get max entry position.
    '''
    pass


def get_no_posion_symbol():
    '''
    Return list of coins with no position
    '''
    pass


def check_symbol_signal(timeframe):
    '''
    Get entry flag each symbol each timeframe.
    '''
    pass


def check_lead_symbol_signal(timeframe):
    '''
    Confirm lead symbols to get entry flag.
    '''
    pass


def check_close_signals(exchange, config_params_path, position):
    '''
    Close position if one of criteria occur.
    Return clost_position flag.
    '''
    is_close_position = False
    price_signal, supertrend_signal, wt_cross_signal = get_signal(exchange, config_params_path, position)

    if price_signal or supertrend_signal or wt_cross_signal:
        is_close_position = True        

    return is_close_position


def check_long_entry_position_condition():
    '''
    Close position if one of criteria occur.
    Return clost_position flag.
    '''
    get_signal()
    pass


def check_short_entry_position_condition():
    '''
    Close position if one of criteria occur.
    Return clost_position flag.
    '''
    get_signal()
    pass


def close_position(exchange, symbol, action, last_loop_dict):
    '''
    '''
    amount = last_loop_dict['symbols'][symbol]
    order = exchange.create_order(symbol, 'market', action, amount)

    return order


def create_action(position, to_close=True):
    '''
    '''
    if (to_close and position == 'long') or (not to_close and position == 'short'):
        action = 'sell'
    elif (to_close and position == 'short') or (not to_close and position == 'long'):
        action = 'buy'
    else:
        raise ValueError('position should be long or short only!!!')

    return action


def clear_position_symbol(symbol, last_loop_dict):
    '''
    Remove position of symbol before entering
    a new position.
    '''
    last_loop_dict.pop(symbol)


def check_close_position(exchange, config_params_path, last_loop_path, transactions_df_path, profit_df_path):
    '''
    Check all coins in last_loop
    if exist:
        check_close_position_condition()
            if match condition:
                Close order()
                append_order(transaction_file)
                append_profit_technical(profit_file)
    else:
        Fetch lastest symbol transaction
        append_order(transaction_file)
        append_profit_technical(profit_file)

    Remove symbol from last_loop
    '''
    last_loop = get_json(last_loop_path)
    symbols = list(last_loop['symbols'].keys())

    for symbol in symbols:
        position = symbol['position']
        is_close_position = check_close_signals(exchange, config_params_path, position)
            
        if is_close_position:
            action = create_action(position, to_close=True)
            order = close_position(exchange, symbol, action, last_loop)

            append_order(order, 'filled', transactions_df_path)    
            append_profit_technical(exchange, order, last_loop, profit_df_path)

            clear_position_symbol(symbol, last_loop) 


def check_open_position():
    '''
    
    '''
    n_order = cal_budget_technical()
    coin_list = get_no_posion_symbol()

    for i in range(min(n_order, len(coin_list))):
        long_entry_position_flag = check_long_entry_position_condition()

        if long_entry_position_flag == 1:
            # Open long position
            pass
        else:
            short_entry_position_flag = check_short_entry_position_condition()

            if short_entry_position_flag == 1:
                # Open short position
                pass


def update_end_date_technical(prev_date, bot_name, exchange, config_params_path, last_loop_path, transfer_path, cash_flow_df_path, profit_df_path):
    '''
    Update cash_flow file.
    Change budget in last_loop file if transfer occur.
    '''
    config_params = get_json(config_params_path)
    last_loop = get_json(last_loop_path)
    transfer = get_json(transfer_path)
    
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)

    symbols = symbols = list(last_loop['symbols'].keys()) 

    unrealised = 0
    base_currency_value = 0
    quote_currency_value = 0

    for symbol in symbols:
        last_price = get_last_price(exchange, symbol)
        position = last_loop['symbol']['position'] 
         
        unrealised += cal_unrealised_future(last_price, position)
        
        base_currency_value += get_base_currency_value(last_price, exchange, symbol)
        quote_currency_value += get_quote_currency_value(last_price, exchange)

    end_balance = cal_end_balance(base_currency_value, quote_currency_value, transfer)

    profit_df = pd.read_csv(profit_df_path)
    last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
    profit = sum(last_profit_df['profit'])
    
    cash_flow_list = [
        'prev_date', 
        end_balance, 
        unrealised, 
        profit, 
        transfer['deposit'], 
        transfer['withdraw']
    ]

    append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)
    update_transfer(config_params['taker_fee'], transfer_path)