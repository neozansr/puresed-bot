import ccxt
import pandas as pd
import datetime as dt
from dateutil import tz
import json
import sys


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    run_flag = config_system['run_flag']
    idle_stage = config_system['idle_stage']
    keys_path = config_system['keys_path']

    return run_flag, idle_stage, keys_path


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']
    fix_value = config_params['fix_value']
    min_value = config_params['min_value']

    return symbol, fix_value, min_value


def get_time(timezone = 'Asia/Bangkok'):
    timestamp = dt.datetime.now(tz = tz.gettz(timezone))
    
    return timestamp


def get_date(timezone = 'Asia/Bangkok'):
    timestamp = dt.datetime.now(tz = tz.gettz(timezone))
    date = timestamp.date()
    
    return date


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.ftx({'apiKey': keys_dict['apiKey'],
                         'secret': keys_dict['secret'],
                         'headers': {'FTX-SUBACCOUNT': keys_dict['subaccount']},
                         'enableRateLimit': True})

    return exchange
    

def get_currency(symbol):
    base_currency = symbol.split('/')[0]
    quote_currency = symbol.split('/')[1]

    return base_currency, quote_currency


def get_last_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    last_price = ticker['last']

    _, quote_currency = get_currency(symbol)
    print('Last price: {:.2f} {}'.format(last_price, quote_currency))
    return last_price


def get_bid_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    bid_price = ticker['bid']

    return bid_price


def get_ask_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    ask_price = ticker['bid']

    return ask_price
    

def get_current_value(exchange, symbol, last_price):
    balance = exchange.fetch_balance()
    base_currency, _ = get_currency(symbol)
    
    try:
        amount = balance[base_currency]['total']
        current_value = last_price * amount
    except KeyError:
        current_value = 0

    return current_value


def get_balance(exchange, symbol, last_price):
    balance = exchange.fetch_balance()
    base_currency, quote_currency = get_currency(symbol)
    
    try:
        base_currency_amount = balance[base_currency]['total']
    except KeyError:
        base_currency_amount = 0

    base_currency_value = last_price * base_currency_amount

    try:    
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0
    
    balance = base_currency_value + quote_currency_value
    
    return balance


def gen_series(n = 18, limit_min = 4):
    # haxanacci
    def hexa(n) :
        if n in range(6):
            return 0
        elif n == 6:
            return 1
        else :
            return (hexa(n - 1) +
                    hexa(n - 2) +
                    hexa(n - 3) +
                    hexa(n - 4) +
                    hexa(n - 5) +
                    hexa(n - 6))
    
    series = []
    for i in range(6, n):
        series.append(hexa(i))
        
    series = [x for x in series if x >= limit_min]
    
    if len(series) == 0:
        print('No series generated, increase n size')
        sys.exit(1)
        
    return series


def get_idle_loop(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    n_loop = last_loop_dict['n_loop']
    series = gen_series()
    idle_loop = series[n_loop]

    update_n_loop(n_loop, series, last_loop_dict, last_loop_path)
    
    return idle_loop


def update_n_loop(n_loop, series, last_loop_dict, last_loop_path):
    n_loop += 1
    if n_loop >= len(series):
        n_loop = 0

    last_loop_dict['n_loop'] = n_loop

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def get_withdraw_flag(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    withdraw_flag = last_loop_dict['withdraw_flag']

    return withdraw_flag


def update_withdraw_flag(last_loop_path, enable):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    if enable == True:
        last_loop_dict['withdraw_flag'] = 1
    else:
        last_loop_dict['withdraw_flag'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def get_transfer(transfer_path):
    with open(transfer_path) as transfer_file:
        transfer_dict = json.load(transfer_file)

    deposit = transfer_dict['deposit']
    withdraw = transfer_dict['withdraw']
    withdraw_cash_flow = transfer_dict['withdraw_cash_flow']

    return deposit, withdraw, withdraw_cash_flow


def get_available_cash_flow(withdraw_cash_flow, cash_flow_df):
    try:
        avaialble_cash_flow = cash_flow_df['available_cash_flow'][len(cash_flow_df) - 1]
        avaialble_cash_flow -= withdraw_cash_flow
    except IndexError:
        # first date
        avaialble_cash_flow = 0

    return avaialble_cash_flow


def append_cash_flow_df(prev_date, balance, cash, fix_value, cash_flow, withdraw_cash_flow, available_cash_flow, deposit, withdraw, cash_flow_df, cash_flow_df_path):
    cash_flow_df.loc[len(cash_flow_df)] = [prev_date, balance, cash, fix_value, cash_flow, withdraw_cash_flow, available_cash_flow, deposit, withdraw]
    cash_flow_df.to_csv(cash_flow_df_path, index = False)


def update_fix_value(fix_value, deposit, withdraw, config_params_path):
    fix_value += ((deposit - withdraw) / 2)

    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    config_params['fix_value'] = fix_value

    with open(config_params_path, 'w') as config_params_path:
        json.dump(config_params, config_params_path, indent = 1)


def reset_transfer(transfer_path):
    with open(transfer_path) as transfer_file:
        transfer_dict = json.load(transfer_file)

    transfer_dict['deposit'] = 0
    transfer_dict['withdraw'] = 0

    with open(transfer_path, 'w') as transfer_file:
        json.dump(transfer_dict, transfer_file, indent = 1)


def update_budget(exchange, bot_name, symbol, fix_value, config_params_path, transfer_path, transactions_df_path, profit_df_path, cash_flow_df_path):
    withdraw_flag = 0

    last_price = get_last_price(exchange, symbol)
    current_value = get_current_value(exchange, symbol, last_price)
    
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transactions_df = pd.read_csv(transactions_df_path)

    try:
        last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
        last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    except IndexError:
        last_date = None

    cur_date = get_date()
    prev_date = cur_date - dt.timedelta(days = 1)
    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    # skip 1st date
    if (len(last_transactions_df) > 0) | (len(cash_flow_df) > 0):
        if last_date != prev_date:
            balance = get_balance(exchange, symbol, last_price)
            cash = balance - current_value

            profit_df = pd.read_csv(profit_df_path)
            last_profit_df = profit_df[pd.to_datetime(profit_df['timestamp']).dt.date == prev_date]
            cash_flow = sum(last_profit_df['profit'])
            deposit, withdraw, withdraw_cash_flow = get_transfer(transfer_path)
            
            available_cash_flow = get_available_cash_flow(withdraw_cash_flow, cash_flow_df)
            available_cash_flow += cash_flow
            
            append_cash_flow_df(prev_date, balance, cash, fix_value, cash_flow, withdraw_cash_flow, available_cash_flow, deposit, withdraw, cash_flow_df, cash_flow_df_path)
            update_fix_value(fix_value, deposit, withdraw, config_params_path)
            reset_transfer(transfer_path)

            if  withdraw > deposit:
                withdraw_flag = 1

    return withdraw_flag


def reset_n_loop(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    last_loop_dict['n_loop'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)