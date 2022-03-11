import pandas as pd
import requests

from func_get import get_json, get_time, get_currency, get_base_currency_value, get_cash_value, get_pending_order
from func_cal import cal_unrealised, cal_unrealised_future, cal_drawdown_future


def get_line_message(payload, noti_type, home_path='../'):
    url = 'https://notify-api.line.me/api/notify'
    token_path = home_path + '../_keys/bot_token.json'
    token_dict = get_json(token_path)
    token = token_dict['line'][noti_type]
    
    headers = {'Authorization':'Bearer ' + token}
    
    return requests.post(url, headers=headers , data=payload)

    
def line_send(message, noti_type):
    payload = {'message':message}
    send_message = get_line_message(payload, noti_type)
    
    return send_message


def noti_success_order(order, bot_name, symbol):
    base_currency, quote_currency = get_currency(symbol)
    
    side = order['side']
    filled = order['filled']
    price = order['price']
    
    message = f"{bot_name}: {side} {filled} {base_currency} at {price} {quote_currency}"
    line_send(message, noti_type='order')
    print(message)


def noti_warning(warning, bot_name):
    message = f"{bot_name}: {warning}!!!!!"
    line_send(message, noti_type='warning')
    print(message)