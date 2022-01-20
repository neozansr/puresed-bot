def get_ohlcv():
    '''
    Fetch candle stick from exchange at the longest windows signal
    '''
    pass


def add_ema():
    '''
    Add ema_signal to ohlcv
    '''
    pass


def add_supertrend():
    '''
    Add supertrend to ohlcv
    '''
    pass


def add_wt_cross():
    '''
    Add wt_cross to ohlcv
    '''
    pass


def get_signal():
    get_ohlcv()
    add_ema()
    add_supertrend()
    add_wt_cross()


def append_profit_technical():
    '''
    Cal profit from close order.
    Record profit on profit file.
    '''
    pass


def cal_budget_technical():
    '''
    Get max entry position.
    '''
    pass


def get_no_posion_symbol():
    '''
    Return list of coins with no position
    '''
    pass


def check_symbol_signal(timeframe):
    '''
    Get entry flag each symbol each timeframe.
    '''
    pass


def check_lead_symbol_signal(timeframe):
    '''
    Confirm lead symbols to get entry flag.
    '''
    pass


def check_close_position_condition():
    '''
    Close position if one of criteria occur.
    Return clost_position flag.
    '''
    get_signal()
    pass


def check_long_entry_position_condition():
    '''
    Close position if one of criteria occur.
    Return clost_position flag.
    '''
    get_signal()
    pass


def check_short_entry_position_condition():
    '''
    Close position if one of criteria occur.
    Return clost_position flag.
    '''
    get_signal()
    pass


def check_current_position():
    '''
    Check all coins in last_loop
    if exist:
        check_close_position_condition()
            if match condition:
                Close order()
                append_order(transaction_file)
                append_profit_technical(profit_file)
    else:
        Fetch lastest symbol transaction
        append_order(transaction_file)
        append_profit_technical(profit_file)

    Remove symbol from last_loop
    '''
    pass


def open_position():
    n_order = cal_budget_technical()
    coin_list = get_no_posion_symbol()

    for i in range(min(n_order, len(coin_list))):
        long_entry_position_flag = check_long_entry_position_condition()

        if long_entry_position_flag == 1:
            # Open long position
            pass
        else:
            short_entry_position_flag = check_short_entry_position_condition()

            if short_entry_position_flag == 1:
                # Open short position
                pass


def update_end_date_technical():
    '''
    Update cash_flow file.
    Change budget in last_loop file if transfer occur.
    '''
    pass