import json
import requests

from func_get import get_balance


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


def print_current_balance(exchange, current_value, symbol, quote_currency, last_price):
    balance = get_balance(exchange, symbol, last_price)
    cash = balance - current_value

    print('Balance: {:.2f} {}'.format(balance, quote_currency))
    print('Cash: {:.2f} {}'.format(cash, quote_currency))