import numpy as np
import pandas as pd
import math
import datetime as dt


def get_signal_dict(signal, objective, timeframe, config_params):
    if objective in ['open', 'close']:
        signal_dict = config_params['base'][objective][timeframe][signal]
    elif objective in ['tp', 'sl']:
        signal_dict = config_params[objective]['signal']['signal'][signal]

    return signal_dict


def get_signal_side(ohlcv_df, signal):
    if ohlcv_df['close'] > ohlcv_df[signal]:
        signal_side = 'buy'
    elif ohlcv_df['close'] < ohlcv_df[signal]:
        signal_side = 'sell'
    else:
        signal_side = None
        
    return signal_side


def get_signal_cross_side(ohlcv_df, signal):
    if ohlcv_df[f'short_{signal}'] > ohlcv_df[f'long_{signal}']:
        signal_side = 'buy'
    elif ohlcv_df[f'short_{signal}'] < ohlcv_df[f'long_{signal}']:
        signal_side = 'sell'
    else:
        signal_side = None
        
    return signal_side


def get_signal_bound_side(ohlcv_df):
    if ohlcv_df['close'] > ohlcv_df['max_high']:
        signal_side = 'buy'
    elif ohlcv_df['close'] < ohlcv_df['min_low']:
        signal_side = 'sell'
    else:
        signal_side = np.nan

    return signal_side


def get_signal_equence_side(ohlcv_df, signal):
    if ohlcv_df[signal] > ohlcv_df[f'{signal}_prev']:
        signal_side = 'buy'
    else:
        signal_side = 'sell'

    return signal_side


def revert_signal(action_side):
    if action_side == 'buy':
        action_side = 'sell'
    elif action_side == 'sell':
        action_side = 'buy'

    return action_side


def check_dependent_signal(func):
    def inner(action_list, indicator, upperband, lowerband):
        if len(action_list) == 0:
            raise ValueError("Main signal must be added.")
        else:
            return func(action_list, indicator, upperband, lowerband)

    return inner


def check_signal_side(objective, symbol_type, time, signal, action_list, ohlcv_df, timeframe, config_params):
    check_df = ohlcv_df[ohlcv_df['time'] <= time].reset_index(drop=True)
    check_series = check_df.loc[len(check_df) - 1, :]
    
    action_side = check_series[f'{signal}_side']

    if config_params[symbol_type][objective][timeframe][signal]['revert']:
        action_side = revert_signal(action_side)
    
    action_list.append(action_side)
    return action_side


def check_signal_side_change(objective, symbol_type, time, signal, action_list, ohlcv_df, timeframe, config_params):
    look_back = config_params[symbol_type][objective][timeframe][signal]['look_back']
    check_df = ohlcv_df[ohlcv_df['time'] <= time]
    check_df = check_df.loc[len(check_df) - look_back - 1:].reset_index(drop=True)
    
    if len(check_df) < look_back + 1:
        action_side = 'no_action'
    else:
        action_side_first = check_df.loc[0, f'{signal}_side']
        action_side_unique = check_df.loc[1:len(check_df) - 1, f'{signal}_side'].unique()
        if (len(action_side_unique) == 1) & (action_side_first != action_side_unique[0]):
            action_side = action_side_unique[0]
        else:
            action_side = 'no_action' if objective == 'open' else check_df.loc[len(check_df) - 1, f'{signal}_side']
        
    if config_params[symbol_type][objective][timeframe][signal]['revert']:
        action_side = revert_signal(action_side)

    action_list.append(action_side)
    return action_side


def check_signal_band(objective, symbol_type, time, signal, action_list, ohlcv_df, timeframe, config_params):
    def cal_outer_band(indicator, upperband, lowerband):
        if indicator <= lowerband:
            action_side = 'buy'
        elif indicator >= upperband:
            action_side = 'sell'
        else:
            action_side = 'no_action'

        return action_side


    @check_dependent_signal
    def cal_inner_band(action_list, indicator, upperband, lowerband):
        if (action_list[-1] == 'buy') & (indicator < upperband):
            action_side = 'buy'
        elif (action_list[-1] == 'buy') & (indicator >= upperband):
            action_side = 'sell'
        elif (action_list[-1] == 'sell') & (indicator > lowerband):
            action_side = 'sell'
        elif (action_list[-1] == 'sell') & (indicator <= lowerband):
            action_side = 'buy'
        else:
            action_side = 'no_action'

        return action_side


    check_df = ohlcv_df[ohlcv_df['time'] <= time].reset_index(drop=True)
    check_series = check_df.loc[len(check_df) - 1, :]

    band_type_dict = {
        'signal': ['rsi', 'wt'],
        'price': ['bollinger']
    }

    if signal in band_type_dict['signal']:
        indicator = check_series[signal]
        upperband = config_params[symbol_type][objective][timeframe][signal]['overbought']
        lowerband = config_params[symbol_type][objective][timeframe][signal]['oversold']
    elif signal in band_type_dict['price']:
        indicator = check_series['close']
        upperband = check_series[f'{signal}_upper']
        lowerband = check_series[f'{signal}_lower']
    
    if config_params[symbol_type][objective][timeframe][signal]['trigger'] == 'outer':
        action_side = cal_outer_band(indicator, upperband, lowerband)
    elif config_params[symbol_type][objective][timeframe][signal]['trigger'] == 'inner':
        action_side = cal_inner_band(action_list, indicator, upperband, lowerband)

    if config_params[symbol_type][objective][timeframe][signal]['revert']:
        action_side = revert_signal(action_side)
        
    action_list.append(action_side)
    return action_side


def call_check_signal_func(func_name):
    check_func_dict = {
        'check_signal_side': check_signal_side,
        'check_signal_side_change': check_signal_side_change,
        'check_signal_band': check_signal_band
    }

    return check_func_dict[func_name]


def cal_sma(ohlcv_df, windows):
    sma_list = ohlcv_df['close'].rolling(window=windows).mean()

    return sma_list


def cal_ema(ohlcv_df, windows):
    ema_list = ohlcv_df['close'].ewm(span=windows, adjust=False).mean()

    return ema_list


def cal_tma(ohlcv_df, windows):
    sub_interval = (windows + 1) / 2
    temp_df = ohlcv_df.copy()
    
    sma_list = temp_df['close'].rolling(window=math.trunc(sub_interval)).mean()
    temp_df['ma'] = sma_list
    
    tma_list = temp_df['ma'].rolling(window=int(np.round(sub_interval))).mean()

    return tma_list


def cal_wma(ohlcv_df, input_col, windows):
    def weight_ma(series):
        weight_list = list(range(1, len(series) + 1))
        weighted_average_list = [i * j for i, j in zip(series, weight_list)]

        wma = sum(weighted_average_list) / sum(weight_list)

        return wma
    
    wma_list = [None] * (windows - 1)

    for i in range(len(ohlcv_df) - (windows - 1)):
        close_series = ohlcv_df.loc[i:i + (windows - 1), input_col]
        wma = weight_ma(close_series)
        wma_list.append(wma)

    return wma_list


def cal_atr(ohlcv_df, atr_range):
    temp_df = ohlcv_df.copy()

    high_low = temp_df['high'] - temp_df['low']
    high_close = np.abs(temp_df['high'] - temp_df['close'].shift(periods=1))
    low_close = np.abs(temp_df['low'] - temp_df['close'].shift(periods=1))

    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    temp_df['true_range'] = true_range
    atr_list = temp_df['true_range'].ewm(alpha=1 / atr_range, min_periods=atr_range).mean()

    return atr_list


def cal_basic_band(ohlcv_df, index, multiplier):
    mid_price = (ohlcv_df.loc[index, 'high'] + ohlcv_df.loc[index, 'low']) / 2
    default_atr = multiplier * ohlcv_df.loc[index, 'atr']
    basic_upperband = mid_price + default_atr
    basic_lowerband = mid_price - default_atr

    return basic_upperband, basic_lowerband


def cal_final_upperband(ohlcv_df, index, basic_upperband, final_upperband_list):
    if (basic_upperband < final_upperband_list[index - 1]) | (ohlcv_df.loc[index - 1, 'close'] > final_upperband_list[index - 1]):
        final_upperband = basic_upperband
    else:
        final_upperband = final_upperband_list[index - 1]

    return final_upperband


def cal_final_lowerband(ohlcv_df, index, basic_lowerband, final_lowerband_list):
    if (basic_lowerband > final_lowerband_list[index - 1]) | (ohlcv_df.loc[index - 1, 'close'] < final_lowerband_list[index - 1]):
        final_lowerband = basic_lowerband
    else:
        final_lowerband = final_lowerband_list[index - 1]

    return final_lowerband


def cal_final_band(ohlcv_df, index, basic_upperband, basic_lowerband, final_upperband_list, final_lowerband_list):
    if len(final_upperband_list) == 0:
        # First loop
        final_upperband = basic_upperband
        final_lowerband = basic_lowerband
    else:
        final_upperband = cal_final_upperband(ohlcv_df, index, basic_upperband, final_upperband_list)
        final_lowerband = cal_final_lowerband(ohlcv_df, index, basic_lowerband, final_lowerband_list)

    return final_upperband, final_lowerband


def cal_supertrend(ohlcv_df, index, final_upperband, final_lowerband, supertrend_side_list):
    def get_last_trend(final_upperband, final_lowerband, supertrend_side_list):
        if len(supertrend_side_list) == 0:
            # First loop
            supertrend_side = None
        else:
            supertrend_side = supertrend_side_list[-1]
            
        if supertrend_side == 'buy':
            supertrend = final_lowerband
        else:
            supertrend = final_upperband

        return supertrend, supertrend_side


    if ohlcv_df.loc[index, 'close'] > final_upperband:
        supertrend = final_lowerband
        supertrend_side = 'buy'
    elif ohlcv_df.loc[index, 'close'] < final_lowerband:
        supertrend = final_upperband
        supertrend_side = 'sell'
    else:
        supertrend, supertrend_side = get_last_trend(final_upperband, final_lowerband, supertrend_side_list)

    return supertrend, supertrend_side


def add_sma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('sma', objective, timeframe, config_params)
    
    ohlcv_df['sma'] = cal_sma(ohlcv_df, signal_dict['windows'])
    ohlcv_df[f'sma_side'] = ohlcv_df.apply(get_signal_side, signal='sma', axis=1)
    
    return ohlcv_df


def add_ema(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('ema', objective, timeframe, config_params)
    
    ohlcv_df['ema'] = cal_ema(ohlcv_df, signal_dict['windows'])
    ohlcv_df['ema_side'] = ohlcv_df.apply(get_signal_side, signal='ema', axis=1)
    
    return ohlcv_df


def add_tma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('tma', objective, timeframe, config_params)

    ohlcv_df['tma'] = cal_tma(ohlcv_df, signal_dict['windows'])
    ohlcv_df['tma_side'] = ohlcv_df.apply(get_signal_side, signal='tma', axis=1)
    
    return ohlcv_df


def add_cross_sma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('cross_sma', objective, timeframe, config_params)
    
    ohlcv_df['short_sma'] = cal_sma(ohlcv_df, signal_dict['short_windows'])
    ohlcv_df['long_sma'] = cal_sma(ohlcv_df, signal_dict['long_windows'])
    ohlcv_df['cross_sma_side'] = ohlcv_df.apply(get_signal_cross_side, signal='sma', axis=1)
    
    return ohlcv_df


def add_cross_ema(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('cross_ema', objective, timeframe, config_params)

    ohlcv_df['short_ema'] = cal_ema(ohlcv_df, signal_dict['short_windows'])
    ohlcv_df['long_ema'] = cal_ema(ohlcv_df, signal_dict['long_windows'])
    ohlcv_df['cross_ema_side'] = ohlcv_df.apply(get_signal_cross_side, signal='ema', axis=1)
    
    return ohlcv_df


def add_cross_tma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('cross_tma', objective, timeframe, config_params)
    
    ohlcv_df['short_tma'] = cal_tma(ohlcv_df, signal_dict['short_windows'])
    ohlcv_df['long_tma'] = cal_tma(ohlcv_df, signal_dict['long_windows'])
    ohlcv_df['cross_tma_side'] = ohlcv_df.apply(get_signal_cross_side, signal='tma', axis=1)
    
    return ohlcv_df


def add_bollinger(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('bollinger', objective, timeframe, config_params)
    
    temp_df = ohlcv_df.copy()

    sma_list = temp_df['close'].rolling(window=signal_dict['windows']).mean()
    std_list = temp_df['close'].rolling(window=signal_dict['windows']).std(ddof=0)
    
    bollinger_upper = sma_list + (std_list * signal_dict['std'])
    bollinger_lower = sma_list - (std_list * signal_dict['std'])
    
    ohlcv_df['bollinger_upper'] = bollinger_upper
    ohlcv_df['bollinger_lower'] = bollinger_lower
    
    return ohlcv_df


def add_wt(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('wt', objective, timeframe, config_params)
    
    temp_df = ohlcv_df.copy()
    
    temp_df['average_price'] = (temp_df['high'] + temp_df['low'] + temp_df['close']) / 3
    temp_df['esa'] = temp_df['average_price'].ewm(span=signal_dict['channel_range']).mean()
    temp_df['dd'] = abs(temp_df['average_price'] - temp_df['esa'])
    temp_df['d'] = temp_df['dd'].ewm(span=signal_dict['channel_range']).mean()
    temp_df['ci'] = (temp_df['average_price'] - temp_df['esa']) / (0.015 * temp_df['d'])
    
    wt_list = temp_df['ci'].ewm(span=signal_dict['average_range']).mean()
    ohlcv_df['wt'] = wt_list
    
    return ohlcv_df


def add_rsi(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('rsi', objective, timeframe, config_params)

    temp_df = ohlcv_df.copy()
    
    temp_df['diff'] = temp_df['close'].diff(1)
    temp_df['gain'] = temp_df['diff'].clip(lower=0)
    temp_df['loss'] = temp_df['diff'].clip(upper=0).abs()

    temp_df['avg_gain'] = temp_df['gain'].rolling(window=signal_dict['average_range'], min_periods=signal_dict['average_range']).mean()[:signal_dict['average_range']+1]
    temp_df['avg_loss'] = temp_df['loss'].rolling(window=signal_dict['average_range'], min_periods=signal_dict['average_range']).mean()[:signal_dict['average_range']+1]

    for i, _ in enumerate(temp_df.loc[signal_dict['average_range'] + 1:, 'avg_gain']):
        temp_df.loc[i + signal_dict['average_range'] + 1, 'avg_gain'] = (temp_df.loc[i + signal_dict['average_range'], 'avg_gain'] * (signal_dict['average_range'] - 1) + temp_df.loc[i + signal_dict['average_range'] + 1, 'gain']) / signal_dict['average_range']

    for i, _ in enumerate(temp_df.loc[signal_dict['average_range'] + 1:, 'avg_loss']):
        temp_df.loc[i + signal_dict['average_range'] + 1, 'avg_loss'] = (temp_df.loc[i + signal_dict['average_range'], 'avg_loss'] * (signal_dict['average_range'] - 1) + temp_df.loc[i + signal_dict['average_range'] + 1, 'loss']) / signal_dict['average_range']

    temp_df['rs'] = temp_df['avg_gain'] / temp_df['avg_loss']
    
    rsi_list = 100 - (100 / (1.0 + temp_df['rs']))
    ohlcv_df['rsi'] = rsi_list
  
    return ohlcv_df


def add_supertrend(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('supertrend', objective, timeframe, config_params)

    temp_df = ohlcv_df.copy()

    temp_df['atr'] = cal_atr(ohlcv_df, signal_dict['atr_range'])
    temp_df = temp_df.dropna().reset_index(drop=True)
    
    supertrend_list = []
    supertrend_side_list = []
    final_upperband_list = []
    final_lowerband_list = []

    for i in range(len(temp_df)):
        basic_upperband, basic_lowerband = cal_basic_band(temp_df, i, signal_dict['multiplier'])
        final_upperband, final_lowerband = cal_final_band(temp_df, i, basic_upperband, basic_lowerband, final_upperband_list, final_lowerband_list)
        
        final_upperband_list.append(final_upperband)
        final_lowerband_list.append(final_lowerband)
            
        supertrend, supertrend_side = cal_supertrend(temp_df, i, final_upperband, final_lowerband, supertrend_side_list)
        
        supertrend_side_list.append(supertrend_side)
        supertrend_list.append(supertrend)

    ohlcv_df['supertrend'] = ([None] * (signal_dict['atr_range'] - 1)) + supertrend_list
    ohlcv_df['supertrend_side'] = ([None] * (signal_dict['atr_range'] - 1)) + supertrend_side_list
    
    return ohlcv_df


def add_donchian(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('donchian', objective, timeframe, config_params)
    
    temp_df = ohlcv_df.copy()

    max_high_list = temp_df['high'].rolling(window=signal_dict['windows']).max()
    min_low_list = temp_df['low'].rolling(window=signal_dict['windows']).min()
    
    temp_df['max_high'] = max_high_list
    temp_df['min_low'] = min_low_list
    
    temp_df['max_high'] = temp_df['max_high'].shift(periods=1)
    temp_df['min_low'] = temp_df['min_low'].shift(periods=1)
    temp_df['donchian_side'] = temp_df.apply(get_signal_bound_side, axis=1)
    
    donchian_list = temp_df['donchian_side'].fillna(method='ffill')
    ohlcv_df['donchian_side'] = donchian_list
    
    return ohlcv_df


def add_hull(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('hull', objective, timeframe, config_params)
    
    temp_df = ohlcv_df.copy()

    temp_df['hwma'] = cal_wma(temp_df, 'close', int(round(signal_dict['windows'] / 2)))
    temp_df['wma'] = cal_wma(temp_df, 'close', signal_dict['windows'])
    temp_df['twma'] = (2 * temp_df['hwma']) - temp_df['wma']
    hull_list = cal_wma(temp_df, 'twma', int(round(signal_dict['windows'] ** (1 / 2))))
    
    temp_df['hull'] = hull_list
    temp_df['hull_prev'] = temp_df['hull'].shift(periods=2)
    hull_side_list = temp_df.apply(get_signal_equence_side, signal='hull', axis=1)
    
    ohlcv_df['hull'] = hull_list
    ohlcv_df['hull_side'] = hull_side_list
    
    return ohlcv_df


def add_action_signal(objective, ohlcv_df, symbol_type, timeframe, symbol, func_add_dict, config_params):
    if timeframe in config_params[symbol_type][objective]:
        for signal in config_params[symbol_type][objective][timeframe]:
            if signal not in ohlcv_df.columns:
                print(f"{symbol_type} add {signal} to {symbol} {timeframe}")
                ohlcv_df = func_add_dict[signal](objective, ohlcv_df, timeframe, config_params)

    return ohlcv_df


def add_stop_signal(objective, ohlcv_df, timeframe, symbol, func_add_dict, config_params):
    signal = list(config_params[objective]['signal']['signal'])[0]

    if signal not in ohlcv_df.columns:
        print(f"{objective} add {signal} to {symbol} {timeframe}")
        ohlcv_df = func_add_dict[signal](objective, ohlcv_df, timeframe, config_params)

    return ohlcv_df


def get_action_signal(ohlcv_df_dict, func_add_dict, config_params):
    for symbol_type in ['base', 'lead']:
        for timeframe in ohlcv_df_dict[symbol_type]:
            for symbol in ohlcv_df_dict[symbol_type][timeframe]:
                ohlcv_df = ohlcv_df_dict[symbol_type][timeframe][symbol]
                ohlcv_df = add_action_signal('open', ohlcv_df, symbol_type, timeframe, symbol, func_add_dict, config_params)
                ohlcv_df = add_action_signal('close', ohlcv_df, symbol_type, timeframe, symbol, func_add_dict, config_params)
                
                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict


def get_stop_signal(ohlcv_df_dict, func_add_dict, config_params):
    for objective in ['tp', 'sl']:
        if (config_params[objective]['signal'] != None):
            for symbol in ohlcv_df_dict['base'][config_params[objective]['signal']['timeframe']]:
                timeframe = config_params[objective]['signal']['timeframe']
                ohlcv_df = ohlcv_df_dict['base'][config_params[objective]['signal']['timeframe']][symbol]
                ohlcv_df = add_stop_signal(objective, ohlcv_df, timeframe, symbol, func_add_dict, config_params)
                
                ohlcv_df_dict['base'][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict


def filter_start_time(start_date, ohlcv_df_dict, interval_dict):
    for symbol_type in ['base', 'lead']:
        for timeframe in ohlcv_df_dict[symbol_type]:
            for symbol in ohlcv_df_dict[symbol_type][timeframe]:
                ohlcv_df = ohlcv_df_dict[symbol_type][timeframe][symbol]
                
                first_signal_time = start_date - dt.timedelta(minutes=interval_dict[timeframe])
                ohlcv_df = ohlcv_df[ohlcv_df['time'] >= first_signal_time].dropna().reset_index(drop=True)
                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict


def add_signal(start_date, ohlcv_df_dict, interval_dict, config_params):
    func_add_dict = {
        'sma': add_sma,
        'ema': add_ema,
        'tma': add_tma,
        'cross_sma': add_cross_sma,
        'cross_ema': add_cross_ema,
        'cross_tma': add_cross_tma,
        'bollinger': add_bollinger,
        'supertrend': add_supertrend,
        'wt': add_wt,
        'rsi': add_rsi,
        'donchian': add_donchian,
        'hull': add_hull
    }

    ohlcv_df_dict = get_action_signal(ohlcv_df_dict, func_add_dict, config_params)
    ohlcv_df_dict = get_stop_signal(ohlcv_df_dict, func_add_dict, config_params)
    ohlcv_df_dict = filter_start_time(start_date, ohlcv_df_dict, interval_dict)

    return ohlcv_df_dict