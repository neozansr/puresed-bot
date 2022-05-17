from re import S
import numpy as np

from func_get import get_quote_currency_free


def round_amount(amount, exchange, symbol, round_direction):
    rounded_amount_str = exchange.amount_to_precision(symbol, amount)

    if round_direction == 'down':
        rounded_amount = float(rounded_amount_str)
    elif round_direction == 'up':
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


def cal_available_cash(exchange, cash_flow, funding_payment, reserve, config_params, transfer):
    quote_currency_free = get_quote_currency_free(exchange, config_params['symbol'])
    # Exclude withdraw_reserve as it is moved instantly.

    total_withdraw = transfer['withdraw'] + transfer['pending_withdraw']
    available_budget = quote_currency_free - cash_flow - funding_payment - reserve - total_withdraw

    return available_budget


def cal_end_balance(base_currency_value, quote_currency_value, transfer):
    '''
    This function is called before update, today withdraw is still in withdraw.
    After update, withdraw is moved to pending_withdraw and will be edited manually after tranfer fund.
    Deposit has already been included in balance as it is added before end of the day.
    '''
    end_balance = base_currency_value + quote_currency_value - transfer['withdraw'] - transfer['pending_withdraw']

    return end_balance


def cal_end_cash(quote_currency_value, transfer):
    end_cash = quote_currency_value - transfer['withdraw'] - transfer['pending_withdraw']

    return end_cash