import ccxt
import pandas as pd
import json

from func_cal import cal_unrealised


def get_config_params(bot_type, config_params_path):
    with open(config_params_path) as config_file:
        config_params = json.load(config_file)

    symbol = config_params['symbol']

    if bot_type == 'grid':
        grid = config_params['grid']
    else:
        grid = None

    return symbol, grid


def get_keys_path(config_system_path):
    with open(config_system_path) as config_file:
        config_system = json.load(config_file)

    keys_path = config_system['keys_path']

    return keys_path


def get_exchange(keys_path):
    with open(keys_path) as keys_file:
        keys_dict = json.load(keys_file)
    
    exchange = ccxt.kucoin({'apiKey': keys_dict['apiKey'],
                            'secret': keys_dict['secret'],
                            'password': keys_dict['password'],
                            'enableRateLimit': True})

    return exchange


def get_latest_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    latest_price = ticker['last']

    return latest_price


def get_coin_name(symbol):
    trade_coin = symbol.split('/')[0]
    ref_coin = symbol.split('/')[1]

    return trade_coin, ref_coin    


def get_current_value(exchange, latest_price, trade_coin):
    balance = exchange.fetch_balance()
    
    try:
        amount = balance[trade_coin]['total']
        current_value = latest_price * amount
    except KeyError:
        current_value = 0

    return current_value


def get_balance(exchange, latest_price, trade_coin, ref_coin, config_system_path):
    balance = exchange.fetch_balance()

    try:
        trade_coin_amount = balance[trade_coin]['total']
    except KeyError:
        trade_coin_amount = 0

    trade_coin_value = latest_price * trade_coin_amount

    try:    
        ref_coin_value = balance[ref_coin]['total']
    except KeyError:
        ref_coin_value = 0
    
    total_balance = trade_coin_value + ref_coin_value

    return total_balance


def get_hold_assets(grid, latest_price, open_orders_df):
    unrealised_loss, n_open_sell_oders, amount, avg_price = cal_unrealised(grid, latest_price, open_orders_df)

    return unrealised_loss, n_open_sell_oders, amount, avg_price


def get_pending_order(ref_coin, open_orders_df):
    open_buy_orders_df = open_orders_df[open_orders_df['side'] == 'buy']
    min_buy_price = min(open_buy_orders_df['price'], default = 0)
    max_buy_price = max(open_buy_orders_df['price'], default = 0)

    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    min_sell_price = min(open_sell_orders_df['price'], default = 0)
    max_sell_price = max(open_sell_orders_df['price'], default = 0)

    return min_buy_price, max_buy_price, min_sell_price, max_sell_price


def get_grid_text(text, bot_type, sub_path, config_system_path, config_params_path, open_orders_df_path):
    keys_path = get_keys_path(sub_path + config_system_path)
    exchange = get_exchange(keys_path)

    symbol, grid = get_config_params(bot_type, sub_path + config_params_path)
    trade_coin, ref_coin = get_coin_name(symbol)
    latest_price = get_latest_price(exchange, symbol)
    open_orders_df = pd.read_csv(sub_path + open_orders_df_path)

    balance = get_balance(exchange, latest_price, trade_coin, ref_coin, config_system_path)
    unrealised_loss, n_open_sell_oders, amount, avg_price = get_hold_assets(grid, latest_price, open_orders_df)
    min_buy_price, max_buy_price, min_sell_price, max_sell_price = get_pending_order(ref_coin, open_orders_df)

    text += '\nBalance: {:.2f} {}'.format(balance, ref_coin)
    text += '\nHold {:.2f} {} with {} orders at {:.2f} {}'.format(amount, trade_coin, n_open_sell_oders, avg_price, ref_coin)
    text += '\nUnrealised: {:.2f} {}'.format(unrealised_loss, ref_coin)
    text += '\nMin buy price: {:.2f} {}'.format(min_buy_price, ref_coin)
    text += '\nMax buy price: {:.2f} {}'.format(max_buy_price, ref_coin)
    text += '\nMin sell price: {:.2f} {}'.format(min_sell_price, ref_coin)
    text += '\nMax sell price: {:.2f} {}'.format(max_sell_price, ref_coin)

    return text


def get_rebalance_text(text, bot_type, sub_path, config_system_path, config_params_path):
    keys_path = get_keys_path(sub_path + config_system_path)
    exchange = get_exchange(keys_path)

    symbol, _ = get_config_params(bot_type, sub_path + config_params_path)
    trade_coin, ref_coin = get_coin_name(symbol)
    latest_price = get_latest_price(exchange, symbol)

    balance = get_balance(exchange, latest_price, trade_coin, ref_coin, config_system_path)
    current_value = get_current_value(exchange, latest_price, trade_coin)
    cash = balance - current_value

    text += '\nBalance: {:.2f} {}'.format(balance, ref_coin)
    text += '\nCurrent value: {:.2f} {}'.format(current_value, ref_coin)
    text += '\nCash: {:.2f} {}'.format(cash, ref_coin)

    return text