import json
import requests

from func_get import get_balance


def get_line_message(payload, noti_type):
    url = 'https://notify-api.line.me/api/notify'
    
    with open('../../_keys/bot_token.json') as token_file:
        token_dict = json.load(token_file)
    token = token_dict['line'][noti_type]
    
    headers = {'Authorization':'Bearer ' + token}
    
    return requests.post(url, headers=headers , data=payload)


def line_send(message, noti_type):
    payload = {'message':message}
    send_message = get_line_message(payload, noti_type)
    
    return send_message


def noti_success_order(bot_name, order, base_currency, quote_currency):
    side = order['side']
    filled = order['filled']
    price = order['price']

    message = f'{bot_name}: {side} {filled:.3f} {base_currency} at {price:.2f} {quote_currency}'
    line_send(message, noti_type='order')
    print(message)


def print_current_balance_rebalance(exchange, current_value, symbol, quote_currency, last_price):
    balance = get_balance(exchange, symbol, last_price)
    cash = balance - current_value

    print(f'Balance: {balance:.2f} {quote_currency}')
    print(f'Current value: {current_value:.2f} {quote_currency}')
    print(f'Cash: {cash:.2f} {quote_currency}')