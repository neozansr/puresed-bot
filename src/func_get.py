import ccxt
import pandas as pd
import datetime as dt
from dateutil import tz
import json
import requests
from bs4 import BeautifulSoup


def get_json(file_path):
    with open(file_path) as file:
        json_dict = json.load(file)

    return json_dict


def get_time(timezone='Asia/Bangkok'):
    timestamp = dt.datetime.now(tz=tz.gettz(timezone))
    
    return timestamp


def get_date(timezone='Asia/Bangkok'):
    timestamp = dt.datetime.now(tz=tz.gettz(timezone))
    date = timestamp.date()
    
    return date


def convert_tz(utc):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = utc.replace(tzinfo=from_zone).astimezone(to_zone)
    
    return utc


def get_exchange(config_system, future=False):
    with open(config_system['keys_path']) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.ftx({
        'apiKey': keys_dict['apiKey'],
        'secret': keys_dict['secret'],
        'headers': {'FTX-SUBACCOUNT': keys_dict['subaccount']},
        'enableRateLimit': True
        })

    if future == True:
        exchange.options = {'defaultType': 'future'}

    return exchange


def get_currency(config_params):
    base_currency = config_params['symbol'].split('/')[0]
    quote_currency = config_params['symbol'].split('/')[1]

    return base_currency, quote_currency


def get_currency_future(config_params):
    base_currency = config_params['symbol'].split('-')[0]
    quote_currency = 'USD'

    return base_currency, quote_currency


def get_last_price(exchange, config_params):
    ticker = exchange.fetch_ticker(config_params['symbol'])
    last_price = ticker['last']
    
    return last_price


def get_bid_price(exchange, config_params):
    ticker = exchange.fetch_ticker(config_params['symbol'])
    bid_price = ticker['bid']

    return bid_price


def get_ask_price(exchange, config_params):
    ticker = exchange.fetch_ticker(config_params['symbol'])
    ask_price = ticker['ask']

    return ask_price


def get_base_currency_value(last_price, exchange, base_currency):
    balance = exchange.fetch_balance()
    
    try:
        amount = balance[base_currency]['total']
        base_currency_value = last_price * amount
    except KeyError:
        base_currency_value = 0

    return base_currency_value


def get_quote_currency_value(exchange, quote_currency):
    balance = exchange.fetch_balance()

    try:
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0

    return quote_currency_value


def get_pending_order(open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    min_buy_price = min(open_buy_orders_df['price'], default=0)
    max_buy_price = max(open_buy_orders_df['price'], default=0)

    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    min_sell_price = min(open_sell_orders_df['price'], default=0)
    max_sell_price = max(open_sell_orders_df['price'], default=0)

    return min_buy_price, max_buy_price, min_sell_price, max_sell_price


def get_available_cash_flow(transfer, cash_flow_df):
    try:
        avaialble_cash_flow = cash_flow_df['available_cash_flow'][len(cash_flow_df) - 1]
        avaialble_cash_flow -= transfer['withdraw_cash_flow']
    except IndexError:
        # first date
        avaialble_cash_flow = 0

    return avaialble_cash_flow


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


def check_end_date(bot_name, cash_flow_df_path, transactions_df_path):
    cash_flow_df_path = cash_flow_df_path.format(bot_name)
    cash_flow_df = pd.read_csv(cash_flow_df_path)
    transactions_df = pd.read_csv(transactions_df_path)
    
    if len(transactions_df) > 0:
        cur_date = get_date()
        prev_date = cur_date - dt.timedelta(days=1)

        if len(cash_flow_df) > 0:
            last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
        else:
            # first date   
            last_date_str = transactions_df['timestamp'][0]

        last_date = dt.datetime.strptime(last_date_str, '%Y-%m-%d').date()
        
        if last_date != prev_date:
            end_date_flag = 1
        else:
            end_date_flag = 0
    else:
        end_date_flag = 0
        prev_date = None

    return end_date_flag, prev_date