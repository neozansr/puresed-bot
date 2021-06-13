import pandas as pd
import json
import requests

from func_get import get_time, get_currency
from func_cal import cal_unrealised


def line_send(message):
    payload = {'message':message}
    send_message = get_line_message(payload)
    
    return send_message


def get_line_message(payload):
    url = 'https://notify-api.line.me/api/notify'
    
    with open('../../_keys/bot_token.json') as token_file:
        token_dict = json.load(token_file)
    token = token_dict['line']
    
    headers = {'Authorization':'Bearer ' + token}
    
    return requests.post(url, headers = headers , data = payload)


def print_pending_order(symbol, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    _, quote_currency = get_currency(symbol)
    
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    min_buy_price = min(open_buy_orders_df['price'], default = 0)
    max_buy_price = max(open_buy_orders_df['price'], default = 0)

    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    min_sell_price = min(open_sell_orders_df['price'], default = 0)
    max_sell_price = max(open_sell_orders_df['price'], default = 0)

    print('Min buy price: {:.2f} {}'.format(min_buy_price, quote_currency))
    print('Max buy price: {:.2f} {}'.format(max_buy_price, quote_currency))
    print('Min sell price: {:.2f} {}'.format(min_sell_price, quote_currency))
    print('Max sell price: {:.2f} {}'.format(max_sell_price, quote_currency))


def print_hold_assets(symbol, grid, last_price, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    unrealised_loss, n_open_sell_oders, amount, avg_price = cal_unrealised(grid, last_price, open_orders_df)

    assets_dict = {'timestamp': get_time(),
                   'last_price': last_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised_loss': unrealised_loss}

    assets_df = pd.DataFrame(assets_dict, index = [0])
    assets_df.to_csv('assets.csv', index = False)

    base_currency, quote_currency = get_currency(symbol)
    
    print('Hold {:.3f} {} with {} orders at {:.2f} {}'.format(amount, base_currency, n_open_sell_oders, avg_price, quote_currency))
    print('Unrealised: {:.2f} {}'.format(unrealised_loss, quote_currency))


def print_current_balance(exchange, symbol, last_price):
    balance = exchange.fetch_balance()
    base_currency, quote_currency = get_currency(symbol)
    
    try:
        base_currency_amount = balance[base_currency]['total']
        base_currency_value = last_price * base_currency_amount
    except KeyError:
        base_currency_value = 0
    
    try:
        quote_currency_value = balance[quote_currency]['total']
    except KeyError:
        quote_currency_value = 0
    
    total_balance = base_currency_value + quote_currency_value

    print('Balance: {:.2f} {}'.format(total_balance, quote_currency))