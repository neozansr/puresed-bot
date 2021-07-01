import ccxt
import pandas as pd
import datetime as dt
from dateutil import tz
import json
import requests
from bs4 import BeautifulSoup

from func_cal import cal_unrealised


def get_config_system(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    run_flag = config_system['run_flag']
    idle_stage = config_system['idle_stage']
    idle_loop = config_system['idle_loop']
    idle_rest = config_system['idle_rest']
    keys_path = config_system['keys_path']

    return run_flag, idle_stage, idle_loop, idle_rest, keys_path


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']
    init_budget = config_params['init_budget']
    budget = config_params['budget']
    grid = config_params['grid']
    value = config_params['value']
    fluctuation_rate = config_params['fluctuation_rate']
    reinvest_ratio = config_params['reinvest_ratio']
    start_safety = config_params['start_safety']
    circuit_limit = config_params['circuit_limit']
    decimal = config_params['decimal']

    return symbol, init_budget, budget, grid, value, fluctuation_rate, reinvest_ratio, start_safety, circuit_limit, decimal


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
    
    print(f'Last price: {last_price} {quote_currency}')
    return last_price


def get_bid_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    bid_price = ticker['bid']

    return bid_price


def get_ask_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    ask_price = ticker['ask']

    return ask_price


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


def get_last_loop_price(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)
    
    last_loop_price = last_loop_dict['price']

    return last_loop_price


def update_last_loop_price(exchange, symbol, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    last_price = get_last_price(exchange, symbol)
    last_loop_dict['price'] = last_price

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)
    

def get_loss(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)
    
    loss = last_loop_dict['loss']

    return loss


def update_loss(loss, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    loss = last_loop_dict['loss']
    loss -= loss
    last_loop_dict['loss'] = loss

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def reduce_budget(loss, config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)
    
    budget = config_params['budget']
    # loss is negative
    budget += loss
    config_params['budget'] = budget

    with open(config_params_path, 'w') as config_file:
        json.dump(config_params, config_file, indent = 1)


def get_greed_index(default_index = 0.5):
    greed_index = default_index
    
    try:
        URL = 'https://alternative.me/crypto/fear-and-greed-index/'
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, 'html.parser')

        period_class = soup.find_all('div', 'gray')
        index_class = soup.find_all('div', 'fng-circle')

        for p, i in zip(period_class, index_class):
            if p.text == 'Now':
                greed_index = int(i.text)

    except requests.ConnectionError:
        pass

    return greed_index


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


def append_cash_flow_df(prev_date, balance, unrealised, value, cash_flow, reinvest_amount, remain_cash_flow, withdraw_cash_flow, available_cash_flow, loss, deposit, withdraw, cash_flow_df, cash_flow_df_path):
    cash_flow_df.loc[len(cash_flow_df)] = [prev_date, balance, unrealised, value, cash_flow, reinvest_amount, remain_cash_flow, withdraw_cash_flow, available_cash_flow, loss, deposit, withdraw]
    cash_flow_df.to_csv(cash_flow_df_path, index = False)


def update_reinvest(init_budget, new_budget, new_value, config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    config_params['init_budget'] = init_budget
    config_params['budget'] = new_budget
    config_params['value'] = new_value

    with open(config_params_path, 'w') as config_file:
        json.dump(config_params, config_file, indent = 1)


def reset_loss(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop_dict = json.load(last_loop_file)

    last_loop_dict['loss'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop_dict, last_loop_file, indent = 1)


def reset_transfer(transfer_path):
    with open(transfer_path) as transfer_file:
        transfer_dict = json.load(transfer_file)

    transfer_dict['deposit'] = 0
    transfer_dict['withdraw'] = 0
    transfer_dict['withdraw_cash_flow'] = 0

    with open(transfer_path, 'w') as transfer_file:
        json.dump(transfer_dict, transfer_file, indent = 1)


def update_budget(exchange, bot_name, symbol, last_price, init_budget, budget, grid, value, fluctuation_rate, reinvest_ratio, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path):
    change_params_flag = 0

    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transactions_df = pd.read_csv(transactions_df_path)

    try:
        last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
        last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    except IndexError:
        last_date = None
    
    cur_date = get_date()
    prev_date = cur_date - dt.timedelta(days = 1)
    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    if (len(last_transactions_df) > 0) | (len(cash_flow_df) > 0):
        # skip 1st date
        if last_date != prev_date:
            change_params_flag = 1

            balance = get_balance(exchange, symbol, last_price)
            unrealised, _, _, _ = cal_unrealised(last_price, grid, open_orders_df)

            loss = get_loss(last_loop_path)
            last_sell_df = last_transactions_df[last_transactions_df['side'] == 'sell']
            cash_flow = sum(last_sell_df['amount'] * grid)
            
            if reinvest_ratio == -1:
                greed_index = get_greed_index()
                reinvest_ratio = max(1 - (greed_index / 100), 0)

            reinvest_amount = cash_flow * reinvest_ratio
            remain_cash_flow = cash_flow - reinvest_amount

            deposit, withdraw, withdraw_cash_flow = get_transfer(transfer_path)
            lower_price = last_price * (1 - fluctuation_rate)
            n_order = int((last_price - lower_price) / grid)

            init_budget += (deposit - withdraw)
            new_budget = budget + reinvest_amount + deposit - withdraw
            new_value = new_budget / n_order
            
            available_cash_flow = get_available_cash_flow(withdraw_cash_flow, cash_flow_df)
            available_cash_flow += remain_cash_flow

            append_cash_flow_df(prev_date, balance, unrealised, value, cash_flow, reinvest_amount, remain_cash_flow, withdraw_cash_flow, available_cash_flow, loss, deposit, withdraw, cash_flow_df, cash_flow_df_path)
            update_reinvest(init_budget, new_budget, new_value, config_params_path)
            reset_loss(last_loop_path)
            reset_transfer(transfer_path)

    return change_params_flag