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

    return config_system


def get_config_params(config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    return config_params


def get_time(timezone='Asia/Bangkok'):
    timestamp = dt.datetime.now(tz=tz.gettz(timezone))
    
    return timestamp


def get_date(timezone='Asia/Bangkok'):
    timestamp = dt.datetime.now(tz=tz.gettz(timezone))
    date = timestamp.date()
    
    return date


def get_exchange(config_system):
    with open(config_system['keys_path']) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.ftx({'apiKey': keys_dict['apiKey'],
                         'secret': keys_dict['secret'],
                         'headers': {'FTX-SUBACCOUNT': keys_dict['subaccount']},
                         'enableRateLimit': True})

    return exchange


def get_currency(config_params):
    base_currency = config_params['symbol'].split('/')[0]
    quote_currency = config_params['symbol'].split('/')[1]

    return base_currency, quote_currency


def get_last_price(exchange, config_params):
    ticker = exchange.fetch_ticker(config_params['symbol'])
    last_price = ticker['last']

    _, quote_currency = get_currency(config_params['symbol'])
    
    print(f'Last price: {last_price} {quote_currency}')
    return last_price


def get_bid_price(exchange, config_params):
    ticker = exchange.fetch_ticker(config_params['symbol'])
    bid_price = ticker['bid']

    return bid_price


def get_ask_price(exchange, config_params):
    ticker = exchange.fetch_ticker(config_params['symbol'])
    ask_price = ticker['ask']

    return ask_price


def get_balance(exchange, last_price, config_params):
    balance = exchange.fetch_balance()
    base_currency, quote_currency = get_currency(config_params['symbol'])
    
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


def get_last_loop(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    return last_loop


def update_last_loop_price(exchange, symbol, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    last_price = get_last_price(exchange, symbol)
    last_loop['price'] = last_price

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def update_loss(loss, last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    total_loss = last_loop['loss']
    total_loss -= loss
    last_loop['loss'] = total_loss

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def reduce_budget(loss, config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)
    
    budget = config_params['budget']
    # loss is negative
    budget += loss
    config_params['budget'] = budget

    with open(config_params_path, 'w') as config_file:
        json.dump(config_params, config_file, indent=1)


def get_greed_index(default_index=0.5):
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
        transfer = json.load(transfer_file)

    return transfer


def get_available_cash_flow(transfer, cash_flow_df):
    try:
        avaialble_cash_flow = cash_flow_df['available_cash_flow'][len(cash_flow_df) - 1]
        avaialble_cash_flow -= transfer['withdraw_cash_flow']
    except IndexError:
        # first date
        avaialble_cash_flow = 0

    return avaialble_cash_flow


def append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path):
    cash_flow_df.loc[len(cash_flow_df)] = cash_flow_list
    cash_flow_df.to_csv(cash_flow_df_path, index=False)


def update_reinvest(init_budget, new_budget, new_value, config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    config_params['init_budget'] = init_budget
    config_params['budget'] = new_budget
    config_params['value'] = new_value

    with open(config_params_path, 'w') as config_file:
        json.dump(config_params, config_file, indent=1)


def reset_loss(last_loop_path):
    with open(last_loop_path) as last_loop_file:
        last_loop = json.load(last_loop_file)

    last_loop['loss'] = 0

    with open(last_loop_path, 'w') as last_loop_file:
        json.dump(last_loop, last_loop_file, indent=1)


def reset_transfer(transfer_path):
    with open(transfer_path) as transfer_file:
        transfer = json.load(transfer_file)

    for s in transfer.keys():
        transfer[s] = 0

    with open(transfer_path, 'w') as transfer_file:
        json.dump(transfer, transfer_file, indent=1)


def update_budget(exchange, bot_name, last_price, config_params, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path):
    change_params_flag = 0

    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transactions_df = pd.read_csv(transactions_df_path)
    last_loop = get_last_loop(last_loop_path)

    try:
        last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
        last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()
    except IndexError:
        last_date = None
    
    cur_date = get_date()
    prev_date = cur_date - dt.timedelta(days=1)
    last_transactions_df = transactions_df[pd.to_datetime(transactions_df['timestamp']).dt.date == prev_date]

    if ((len(last_transactions_df) > 0) | (len(cash_flow_df) > 0)) & (last_date != prev_date):
        change_params_flag = 1

        balance = get_balance(exchange, last_price, config_params)
        unrealised, _, _, _ = cal_unrealised(last_price, config_params, open_orders_df)

        last_sell_df = last_transactions_df[last_transactions_df['side'] == 'sell']
        cash_flow = sum(last_sell_df['amount'] * config_params['grid'])
        
        if config_params['reinvest_ratio'] == -1:
            greed_index = get_greed_index()
            reinvest_ratio = max(1 - (greed_index / 100), 0)

        reinvest_amount = cash_flow * reinvest_ratio
        remain_cash_flow = cash_flow - reinvest_amount

        transfer = get_transfer(transfer_path)
        lower_price = last_price * (1 - config_params['fluctuation_rate'])
        n_order = int((last_price - lower_price) / config_params['grid'])

        net_transfer = transfer['deposit'] - transfer['withdraw']
        new_init_budget = config_params['init_budget'] + net_transfer
        new_budget = config_params['budget'] + reinvest_amount + net_transfer
        new_value = new_budget / n_order
        
        available_cash_flow = get_available_cash_flow(transfer, cash_flow_df)
        available_cash_flow += remain_cash_flow

        cash_flow_list = [prev_date, balance, unrealised, config_params['value'], cash_flow, reinvest_amount, remain_cash_flow, transfer['withdraw_cash_flow'], available_cash_flow, last_loop['loss'], transfer['deposit'], transfer['withdraw']]
        append_cash_flow_df(cash_flow_list, cash_flow_df, cash_flow_df_path)
        update_reinvest(new_init_budget, new_budget, new_value, config_params_path)
        reset_loss(last_loop_path)
        reset_transfer(transfer_path)

    return change_params_flag