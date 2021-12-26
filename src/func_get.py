import ccxt
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
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
    timestamp = get_time(timezone=timezone)
    date = timestamp.date()
    
    return date


def convert_tz(utc):
    '''
    Transform utc timezone in exchange to local timezone.
    '''
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = utc.replace(tzinfo=from_zone).astimezone(to_zone)
    
    return utc


def get_exchange(config_system, future=False):
    keys_dict = get_json(config_system['keys_path'])
    
    exchange = ccxt.ftx({
        'apiKey': keys_dict['apiKey'],
        'secret': keys_dict['secret'],
        'enableRateLimit': True
        })

    if 'subaccount' in list(keys_dict.keys()):
        exchange.headers = {'FTX-SUBACCOUNT': keys_dict['subaccount'],}

    if future == True:
        exchange.options = {'defaultType': 'future'}

    return exchange


def get_currency(symbol):
    if len(symbol.split('/')) == 2:
        base_currency = symbol.split('/')[0]
        quote_currency = symbol.split('/')[1]
    elif len(symbol.split('-')) == 2:
        base_currency = symbol.split('-')[0]
        quote_currency = 'USD'
    else:
        raise ValueError("Unrecognized symbol pattern")

    return base_currency, quote_currency


def get_last_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    last_price = ticker['last']
    
    return last_price


def get_bid_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    bid_price = ticker['bid']

    return bid_price


def get_ask_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    ask_price = ticker['ask']

    return ask_price


def get_position(exchange, symbol):
    positions = exchange.fetch_positions()
    indexed = exchange.index_by(positions, 'future')
    position = exchange.safe_value(indexed, symbol)

    return position


def get_base_currency_value(last_price, exchange, symbol):
    base_currency, quote_currency = get_currency(symbol)
    
    if quote_currency != 'PERP':
        balance = exchange.fetch_balance()
        
        try:
            amount = balance[base_currency]['total']
            base_currency_value = last_price * amount
        except KeyError:
            base_currency_value = 0

    else:
        try:
            position = get_position(exchange, symbol)
            base_currency_value = last_price * float(position['size'])
        except TypeError:
            base_currency_value = 0

    return base_currency_value


def get_quote_currency_value(exchange, quote_currency):
    balance = exchange.fetch_balance()

    try:
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0

    return quote_currency_value


def get_cash_value(exchange):
    balance = exchange.fetch_balance()
    
    try:
        quote_currency_free = balance['USD']['total']
    except KeyError:
        quote_currency_free = 0

    return quote_currency_free
    

def get_base_currency_free(exchange, base_currency):
    balance = exchange.fetch_balance()
    
    try:
        base_currency_free = balance[base_currency]['free']
    except KeyError:
        base_currency_free = 0

    return base_currency_free


def get_quote_currency_free(exchange, quote_currency):
    balance = exchange.fetch_balance()
    
    try:
        quote_currency_free = balance[quote_currency]['free']
    except KeyError:
        quote_currency_free = 0

    return quote_currency_free


def get_cash_free(exchange):
    balance = exchange.fetch_balance()
    
    try:
        quote_currency_free = balance['USD']['free']
    except KeyError:
        quote_currency_free = 0

    return quote_currency_free


def get_total_value(exchange, config_params):
    total_value = 0
    value_dict = {}
    
    for symbol in config_params['symbol'].keys():
        _, quote_currency = get_currency(symbol)

        last_price = get_last_price(exchange, symbol)
        sub_value = get_base_currency_value(last_price, exchange, symbol)
        side = -1 if config_params['symbol'][symbol] < 0 else 1
        
        value_dict[symbol] = {
            'fix_value':config_params['budget'] * config_params['symbol'][symbol],
            'current_value':sub_value * side
            }

        if quote_currency != 'PERP':    
            total_value += sub_value

    return total_value, value_dict
    

def get_order_fee(order, exchange, symbol):
    # Trades can be queried 200 at most.
    _, quote_currency = get_currency(symbol)

    trades = exchange.fetch_my_trades(symbol, limit=200)
    trades_df = pd.DataFrame(trades)
    order_trade = trades_df[trades_df['order'] == order['id']].reset_index(drop=True)
    
    fee = 0
    
    for i in range(len(order_trade)):
        if (order_trade['fee'][i]['currency'] == quote_currency) | (order_trade['fee'][i]['cost'] == 0):
            fee += order_trade['fee'][i]['cost']
        else:
            # Taker fee should be charged as quote currency.
            # Maker fee should be 0 for FTT stakers.
            raise ValueError("Fee is not quote currency!!!")

    return fee


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
        last_cash_flow = cash_flow_df['available_cash_flow'][len(cash_flow_df) - 1]
    except IndexError:
        # First date
        last_cash_flow = 0

    withdraw_cash_flow = transfer['withdraw_cash_flow']
    avaialble_cash_flow = last_cash_flow - withdraw_cash_flow

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
        prev_date = cur_date - relativedelta(days=1)

        if len(cash_flow_df) > 0:
            last_date_str = cash_flow_df['date'][len(cash_flow_df) - 1]
            last_date = pd.to_datetime(last_date_str).date()
        else:
            # First date   
            last_date_str = transactions_df['timestamp'][0]
            last_date = pd.to_datetime(last_date_str).date() - relativedelta(days=1)
        
        if last_date != prev_date:
            end_date_flag = 1
        else:
            end_date_flag = 0
    else:
        end_date_flag = 0
        prev_date = None

    return end_date_flag, prev_date