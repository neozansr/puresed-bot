import pandas as pd
import json
import requests

from func_get import get_time, get_currency, get_base_currency_value, get_quote_currency_value, get_pending_order
from func_cal import cal_unrealised


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


def noti_success_order(order, bot_name, config_params):
    base_currency, quote_currency = get_currency(config_params)
    side = order['side']
    filled = order['filled']
    price = order['price']
    
    message = f'{bot_name}: {side} {filled:.3f} {base_currency} at {price:.2f} {quote_currency}'
    line_send(message, noti_type='order')
    print(message)


def noti_warning(warning, bot_name):
    message = f'{bot_name}: {warning}!!!!!'
    line_send(message, noti_type='warning')
    print(message)


def print_current_balance(last_price, exchange, config_params):
    base_currency, quote_currency = get_currency(config_params)

    current_value = get_base_currency_value(last_price, exchange, base_currency)
    cash = get_quote_currency_value(exchange, quote_currency)
    balance_value = current_value + cash
    
    print(f'Balance: {balance_value:.2f} {quote_currency}')


def print_hold_assets(last_price, base_currency, quote_currency, config_params, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised(last_price, config_params, open_orders_df)

    assets_dict = {'timestamp': get_time(),
                   'last_price': last_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised': unrealised}

    assets_df = pd.DataFrame(assets_dict, index=[0])
    assets_df.to_csv('assets.csv', index=False)
    
    print(f'Hold {amount:.3f} {base_currency} with {n_open_sell_oders} orders at {avg_price:.2f} {quote_currency}')
    print(f'Unrealised: {unrealised:.2f} {quote_currency}')


def print_pending_order(quote_currency, open_orders_df_path):
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(open_orders_df_path)

    print(f'Min buy price: {min_buy_price:.2f} {quote_currency}')
    print(f'Max buy price: {max_buy_price:.2f} {quote_currency}')
    print(f'Min sell price: {min_sell_price:.2f} {quote_currency}')
    print(f'Max sell price: {max_sell_price:.2f} {quote_currency}')


def print_current_value(current_value, exchange, quote_currency):
    quote_currency_value = get_quote_currency_value(exchange, quote_currency)

    print(f'Current value: {current_value:.2f} {quote_currency}')
    print(f'Cash: {quote_currency_value:.2f} {quote_currency}')


def print_current_value_future(exchange, quote_currency):
    quote_currency_value = get_quote_currency_value(exchange, quote_currency)

    print(f'Balance: {quote_currency_value:.2f} {quote_currency}')


def print_position(last_price, position, quote_currency):
    if position != None:
        side = position['side']
        realised = float(position['realizedPnl'])
        entry_price = float(position['entryPrice'])
        liquidate_price = float(position['estimatedLiquidationPrice'])

        liquidate_percent = max((1 - (entry_price / liquidate_price)) * 100, 0)
        
        print(f'Side: {side}')
        print(f'Realise: {realised}')
        print(f'Last price: {last_price} {quote_currency}')
        print(f'Entry price: {entry_price} {quote_currency}')
        print(f'Liquidate price: {liquidate_price}')
        print(f'Liquidate percent: {liquidate_percent:.2f}%')
    else:
        print('No open position')