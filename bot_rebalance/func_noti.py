import json
import requests

from func_get import get_currency


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


def print_current_balance(exchange, symbol, last_price):
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
    
    total_balance = base_currency_value + quote_currency_value

    print('Balance: {:.2f} {}'.format(total_balance, quote_currency))