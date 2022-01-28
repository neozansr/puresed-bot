import numpy as np


def round_amount(amount, exchange, symbol, type):
    rounded_amount_str = exchange.amount_to_precision(symbol, amount)

    if type == 'down':
        rounded_amount = float(rounded_amount_str)
    elif type == 'up':
        decimal = rounded_amount_str[::-1].find('.')
        min_amount = 10 ** (-decimal)
        rounded_amount = float(rounded_amount_str) + min_amount
    
    return rounded_amount


def round_up_amount(amount, decimal):
    floor_amount = np.ceil(amount * (10 ** decimal)) / (10 ** decimal)
    
    return floor_amount


def cal_adjusted_price(order, fee, side):
    if side == 'buy':
        adjusted_cost = order['cost'] + fee
    elif side == 'sell':
        adjusted_cost = order['cost'] - fee
        
    adjusted_price = adjusted_cost / order['filled']
    
    return adjusted_price
    

def cal_unrealised(last_price, grid, open_orders_df):
    open_sell_orders_df = open_orders_df[open_orders_df['side'] == 'sell']
    n_open_sell_oders = len(open_sell_orders_df)
    
    price_list = [x - grid for x in open_sell_orders_df['price']]
    amount_list = open_sell_orders_df['amount'].to_list()

    amount = sum(amount_list)
    total_value = sum([i * j for i, j in zip(price_list, amount_list)])
    
    try:
        avg_price = total_value / amount
    except ZeroDivisionError:
        avg_price = 0

    unrealised = (last_price - avg_price) * amount

    return unrealised, n_open_sell_oders, amount, avg_price


def cal_unrealised_future(last_price, position):
    '''
    Calculate unrealised balance based on the lastest price.
    '''
    if position['side'] == 'buy':
        margin = last_price - position['entry_price']
    elif position['side'] == 'sell':
        margin = position['entry_price'] - last_price
    
    unrealised = margin * float(position['amount'])

    return unrealised


def cal_drawdown_future(last_price, position):
    if position['side'] == 'buy':
        drawdown = max(1 - (last_price / position['entry_price']), 0)
    elif position['side'] == 'sell':
        drawdown = max((last_price / position['entry_price']) - 1, 0)

    return drawdown
    

def cal_available_budget(quote_currency_free, available_cash_flow, transfer):
    # Exclude withdraw_cash_flow as it is moved instantly.
    total_withdraw = transfer['withdraw'] + transfer['pending_withdraw']
    available_budget = quote_currency_free - available_cash_flow - total_withdraw

    return available_budget


def cal_end_balance(base_currency_value, quote_currency_value, transfer):
    # This function is called before update, today withdraw is still in withdraw.
    # After update, withdraw is moved to pending_withdraw and will be edited manually after tranfer fund.
    # Deposit has already been included in balance as it is added before end of the day.
    end_balance = base_currency_value + quote_currency_value - transfer['withdraw'] - transfer['pending_withdraw']

    return end_balance


def cal_end_cash(cash, transfer):
    end_cash = cash - transfer['withdraw'] - transfer['pending_withdraw']

    return end_cash