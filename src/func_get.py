import ccxt
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from dateutil import tz
import json
import time


def get_json(file_path):
    '''
    Open json file to python dictionary.
    '''
    with open(file_path) as file:
        json_dict = json.load(file)

    return json_dict


def get_time(timezone='Asia/Bangkok'):
    timestamp = dt.datetime.now(tz=tz.gettz(timezone))
    
    return timestamp


def get_date(timezone='Asia/Bangkok'):
    '''
    Get current date.
    '''
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


def get_unix_date(dt_date):
    if isinstance(dt_date, dt.datetime):
        dt_datetime = dt.datetime(dt_date.year, dt_date.month, dt_date.day, dt_date.hour, dt_date.minute)
    elif isinstance(dt_date, dt.date):
        dt_datetime = dt.datetime(dt_date.year, dt_date.month, dt_date.day, 0, 0)
        
    unix_datetime = dt_datetime.timestamp() * 1000
    
    return unix_datetime


def get_exchange(config_system, future=False):
    keys_dict = get_json(config_system['keys_path'])
    
    exchange = ccxt.ftx({
        'apiKey': keys_dict['apiKey'],
        'secret': keys_dict['secret'],
        'enableRateLimit': True
        })

    if 'subaccount' in list(keys_dict):
        exchange.headers = {'FTX-SUBACCOUNT': keys_dict['subaccount']}

    if future == True:
        exchange.options = {'defaultType': 'future'}

    return exchange


def get_last_price(exchange, symbol):
    '''
    Get the lastest price on specific symbol from ticker.
    '''
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


def get_base_currency_amount(exchange, symbol):
    base_currency, _ = get_currency(symbol)
    
    if '-PERP' in symbol:
        try:
            position = get_position(exchange, symbol)
            amount = float(position['size'])
        except TypeError:
            amount = 0
    else:
        balance = exchange.fetch_balance()
        
        try:
            amount = balance[base_currency]['total']
        except KeyError:
            amount = 0

    return amount


def get_base_currency_value(last_price, exchange, symbol):
    '''
    Get lastest value on base currency fetching from exchange.
    '''
    base_currency, _ = get_currency(symbol)
    
    if '-PERP' in symbol:
        try:
            position = get_position(exchange, symbol)
            base_currency_value = last_price * float(position['netSize'])
        except TypeError:
            base_currency_value = 0
    else:
        balance = exchange.fetch_balance()
        
        try:
            amount = balance[base_currency]['total']
            base_currency_value = last_price * amount
        except KeyError:
            base_currency_value = 0

    return base_currency_value


def get_quote_currency_value(exchange, symbol):
    _, quote_currency = get_currency(symbol)
    balance = exchange.fetch_balance()

    try:
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0

    return quote_currency_value
    

def get_base_currency_free(exchange, symbol, open_orders_df_path):
    '''
    Get base_currency_free caused by fee deduction.
    '''
    base_currency, _  = get_currency(symbol)
    
    if '-PERP' in symbol:
        try:
            open_orders_df = pd.read_csv(open_orders_df_path)
            open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
            sum_sell_amount = sum(open_sell_orders_df['amount'])
            position_amount = get_base_currency_amount(exchange, symbol)
            base_currency_free = max(position_amount - sum_sell_amount, 0)
        except TypeError:
            base_currency_free = 0
    else:
        balance = exchange.fetch_balance()
        
        try:
            base_currency_free = balance[base_currency]['free']    
        except KeyError:
            base_currency_free = 0

    return base_currency_free


def get_quote_currency_free(exchange, symbol):
    _, quote_currency = get_currency(symbol)
    balance = exchange.fetch_balance()
    
    try:
        quote_currency_free = balance[quote_currency]['free']
    except KeyError:
        quote_currency_free = 0

    return quote_currency_free
    

def get_order_trade(order, exchange, symbol):
    # Trades can be queried 200 at most.
    trades = exchange.fetch_my_trades(symbol, limit=200)
    trades_df = pd.DataFrame(trades)
    order_trade = trades_df[trades_df['order'] == order['id']].reset_index(drop=True)

    return order_trade


def get_order_fee(order, exchange, symbol, config_system, try_num=5):
    fee = 0
    try_counter = 0

    while try_counter < try_num:
        order_trade = get_order_trade(order, exchange, symbol)

        if len(order_trade) == 0:
            try_counter += 1
            time.sleep(config_system['idle_stage'])
        else:
            try_counter = try_num

    if len(order_trade) > 0:
        fee_currency = order_trade['fee'][0]['currency']
        
        for i in range(len(order_trade)):
            fee += order_trade['fee'][i]['cost']

            if order_trade['fee'][i]['currency'] != fee_currency:
                raise ValueError("Different currency fee!!!")
    else:
        fee_currency = 'USD'

    return fee, fee_currency


def get_pending_order(open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    min_buy_price = min(open_buy_orders_df['price'], default=0)
    max_buy_price = max(open_buy_orders_df['price'], default=0)

    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    min_sell_price = min(open_sell_orders_df['price'], default=0)
    max_sell_price = max(open_sell_orders_df['price'], default=0)

    return min_buy_price, max_buy_price, min_sell_price, max_sell_price


def get_reserve(transfer, cash_flow_df):
    try:
        last_reserve = cash_flow_df['reserve'][len(cash_flow_df) - 1]
    except IndexError:
        # First date
        last_reserve = 0

    withdraw_reserve = transfer['withdraw_reserve']
    reserve = last_reserve - withdraw_reserve

    return reserve


def get_funding_payment(exchange, range):
    if range == 'end_date':
        request = {
            'start_time': int(get_unix_date(dt.date.today() - relativedelta(days=1)) / 1000),
            'end_time': int(get_unix_date(dt.date.today() - relativedelta(seconds=1)) / 1000)
            }
    elif range == 'today':
        request = {
            'start_time': int(get_unix_date(dt.date.today()) / 1000)
            }

    funding_df = pd.DataFrame(exchange.private_get_funding_payments(request)['result'])
    funding_dict = {}

    try:
        funding_df['payment'] = funding_df['payment'].astype(float)

        for symbol in funding_df['future'].unique():
            sub_payment = funding_df.loc[funding_df['future'] == symbol, 'payment'].sum()
            funding_dict[symbol] = sub_payment

        funding_payment = sum(funding_df['payment'])
    except KeyError:
        funding_payment = 0
    
    return funding_payment, funding_dict
    

def check_end_date(cash_flow_df_path, transactions_df_path):
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
            end_date_flag = True
        else:
            end_date_flag = False
    else:
        end_date_flag = False
        prev_date = None

    return end_date_flag, prev_date