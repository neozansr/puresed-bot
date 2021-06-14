import pandas as pd
import json
import requests

from func_get import get_time, get_currency, get_balance
from func_cal import cal_unrealised


def line_send(message, noti_type):
    payload = {'message':message}
    send_message = get_line_message(payload, noti_type)
    
    return send_message


def get_line_message(payload, noti_type):
    url = 'https://notify-api.line.me/api/notify'
    
    with open('../../_keys/bot_token.json') as token_file:
        token_dict = json.load(token_file)
    token = token_dict['line'][noti_type]
    
    headers = {'Authorization':'Bearer ' + token}
    
    return requests.post(url, headers = headers , data = payload)


def print_pending_order(symbol, quote_currency, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    
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


def print_hold_assets(symbol, base_currency, quote_currency, grid, last_price, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    unrealised_loss, n_open_sell_oders, amount, avg_price = cal_unrealised(grid, last_price, open_orders_df)

    assets_dict = {'timestamp': get_time(),
                   'last_price': last_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised_loss': unrealised_loss}

    assets_df = pd.DataFrame(assets_dict, index = [0])
    assets_df.to_csv('assets.csv', index = False)
    
    print('Hold {:.3f} {} with {} orders at {:.2f} {}'.format(amount, base_currency, n_open_sell_oders, avg_price, quote_currency))
    print('Unrealised: {:.2f} {}'.format(unrealised_loss, quote_currency))


def print_current_balance(exchange, symbol, quote_currency, last_price):
    balance = get_balance(exchange, symbol, last_price)

    print('Balance: {:.2f} {}'.format(balance, quote_currency))