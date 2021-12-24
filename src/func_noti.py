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
    
    message = f"{bot_name}: {side} {filled:.3f} {base_currency} at {price:.2f} {quote_currency}"
    line_send(message, noti_type='order')
    print(message)


def noti_warning(warning, bot_name):
    message = f"{bot_name}: {warning}!!!!!"
    line_send(message, noti_type='warning')
    print(message)


def print_current_balance(last_price, exchange, symbol):
    _, quote_currency = get_currency(symbol)

    current_value = get_base_currency_value(last_price, exchange, symbol)
    cash = get_cash_value(exchange)
    balance_value = current_value + cash
    
    print(f"Balance: {balance_value:.2f} {quote_currency}")


def print_hold_assets(last_price, base_currency, quote_currency, grid, open_orders_df_path):
    open_orders_df = pd.read_csv(open_orders_df_path)
    unrealised, n_open_sell_oders, amount, avg_price = cal_unrealised(last_price, grid, open_orders_df)

    assets_dict = {'timestamp': get_time(),
                   'last_price': last_price, 
                   'avg_price': avg_price, 
                   'amount': amount, 
                   'unrealised': unrealised}

    assets_df = pd.DataFrame(assets_dict, index=[0])
    assets_df.to_csv('assets.csv', index=False)
    
    print(f"Hold {amount:.3f} {base_currency} with {n_open_sell_oders} orders at {avg_price:.2f} {quote_currency}")
    print(f"Unrealised: {unrealised:.2f} {quote_currency}")


def print_pending_order(quote_currency, open_orders_df_path):
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(open_orders_df_path)

    print(f"Min buy price: {min_buy_price:.2f} {quote_currency}")
    print(f"Max buy price: {max_buy_price:.2f} {quote_currency}")
    print(f"Min sell price: {min_sell_price:.2f} {quote_currency}")
    print(f"Max sell price: {max_sell_price:.2f} {quote_currency}")
    

def print_position(last_price, position, position_api, quote_currency):
    liquidate_price = float(position_api['estimatedLiquidationPrice'])
    notional_value = float(position_api['cost'])
    unrealised = cal_unrealised_future(last_price, position)
    drawdown = cal_drawdown_future(last_price, position)
    
    print(f"Side: {position['side']}")
    print(f"Unrealise: {unrealised:.2f} {quote_currency}")
    print(f"Last price: {last_price:.2f} {quote_currency}")
    print(f"Entry price: {position['entry_price']:.2f} {quote_currency}")
    print(f"Liquidate price: {liquidate_price:.2f} {quote_currency}")
    print(f"Notional value: {notional_value:.2f} {quote_currency}")
    print(f"Drawdown: {drawdown * 100:.2f}%")