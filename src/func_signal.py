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


def revert_signal(action_side):
    if action_side == 'buy':
        action_side = 'sell'
    elif action_side == 'sell':
        action_side = 'buy'

    return action_side


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

    if look_back > 0:
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


    def cal_inner_band(action_list, indicator, upperband, lowerband):
        if (len(action_list) >= 1):
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
        else:
            raise ValueError("Must be used with other signals")

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


def cal_sma(ohlcv_df, windows, signal):
    temp_df = ohlcv_df.copy()

    sma_list = temp_df['close'].rolling(window=windows).mean()
    ohlcv_df[signal] = sma_list

    return ohlcv_df


def cal_ema(ohlcv_df, windows, signal):
    temp_df = ohlcv_df.copy()

    ema_list = temp_df['close'].ewm(span=windows, adjust=False).mean()
    ohlcv_df[signal] = ema_list

    return ohlcv_df


def cal_tma(ohlcv_df, windows, signal):
    sub_interval = (windows + 1) / 2

    temp_df = ohlcv_df.copy()
    
    sma_list = temp_df['close'].rolling(window=math.trunc(sub_interval)).mean()
    temp_df['ma'] = sma_list
    
    tma_list = temp_df['ma'].rolling(window=int(np.round(sub_interval))).mean()
    ohlcv_df[signal] = tma_list

    return ohlcv_df


def add_sma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('sma', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    ohlcv_df = cal_sma(ohlcv_df, windows, signal='sma')
    ohlcv_df[f'sma_side'] = ohlcv_df.apply(get_signal_side, signal='sma', axis=1)
    
    return ohlcv_df


def add_ema(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('ema', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    ohlcv_df = cal_ema(ohlcv_df, windows, signal='ema')
    ohlcv_df['ema_side'] = ohlcv_df.apply(get_signal_side, signal='ema', axis=1)
    
    return ohlcv_df


def add_tma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('tma', objective, timeframe, config_params)
    windows = signal_dict['windows']

    ohlcv_df = cal_tma(ohlcv_df, windows, signal='tma')
    ohlcv_df['tma_side'] = ohlcv_df.apply(get_signal_side, signal='tma', axis=1)
    
    return ohlcv_df


def add_cross_sma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('cross_sma', objective, timeframe, config_params)
    short_windows = signal_dict['short_windows']
    long_windows = signal_dict['long_windows']
    
    ohlcv_df = cal_sma(ohlcv_df, short_windows, signal='short_sma')
    ohlcv_df = cal_sma(ohlcv_df, long_windows, signal='long_sma')
    ohlcv_df['cross_sma_side'] = ohlcv_df.apply(get_signal_cross_side, signal='sma', axis=1)
    
    return ohlcv_df


def add_cross_ema(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('cross_ema', objective, timeframe, config_params)
    short_windows = signal_dict['short_windows']
    long_windows = signal_dict['long_windows']

    ohlcv_df = cal_ema(ohlcv_df, short_windows, signal='short_ema')
    ohlcv_df = cal_ema(ohlcv_df, long_windows, signal='long_ema')
    ohlcv_df['cross_ema_side'] = ohlcv_df.apply(get_signal_cross_side, signal='ema', axis=1)
    
    return ohlcv_df


def add_cross_tma(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('cross_tma', objective, timeframe, config_params)
    short_windows = signal_dict['short_windows']
    long_windows = signal_dict['long_windows']
    
    ohlcv_df = cal_tma(ohlcv_df, short_windows, signal='short_tma')
    ohlcv_df = cal_tma(ohlcv_df, long_windows, signal='long_tma')
    ohlcv_df['cross_tma_side'] = ohlcv_df.apply(get_signal_cross_side, signal='tma', axis=1)
    
    return ohlcv_df


def add_bollinger(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('bollinger', objective, timeframe, config_params)
    windows = signal_dict['windows']
    std = signal_dict['std']
    
    temp_df = ohlcv_df.copy()

    sma_list = temp_df['close'].rolling(window=windows).mean()
    std_list = temp_df['close'].rolling(window=windows).std(ddof=0)
    
    bollinger_upper = sma_list + (std_list * std)
    bollinger_lower = sma_list - (std_list * std)
    
    ohlcv_df['bollinger_upper'] = bollinger_upper
    ohlcv_df['bollinger_lower'] = bollinger_lower
    
    return ohlcv_df


def add_supertrend(objective, ohlcv_df, timeframe, config_params):
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


    def cal_final_band(ohlcv_df, index, basic_upperband, basic_lowerband, final_upperband_list, final_lowerband_list):
        try:
            if (basic_upperband < final_upperband_list[index - 1]) | (ohlcv_df.loc[i - 1, 'close'] > final_upperband_list[index - 1]):
                final_upperband = basic_upperband
            else:
                final_upperband = final_upperband_list[index - 1]
                
            if (basic_lowerband > final_lowerband_list[index - 1]) | (ohlcv_df.loc[i - 1, 'close'] < final_lowerband_list[index - 1]):
                final_lowerband = basic_lowerband
            else:
                final_lowerband = final_lowerband_list[index - 1]
        except IndexError:
            # First loop
            final_upperband = basic_upperband
            final_lowerband = basic_lowerband

        return final_upperband, final_lowerband


    def cal_supertrend(ohlcv_df, index, final_upperband, final_lowerband, supertrend_side_list):
        if ohlcv_df.loc[index, 'close'] > final_upperband:
            supertrend = final_lowerband
            supertrend_side = 'buy'
        elif ohlcv_df.loc[index, 'close'] < final_lowerband:
            supertrend = final_upperband
            supertrend_side = 'sell'
        else:
            try:
                supertrend_side = supertrend_side_list[-1]
            except IndexError:
                # First loop
                supertrend_side = None
                
            if supertrend_side == 'buy':
                supertrend = final_lowerband
            else:
                supertrend = final_upperband

        return supertrend, supertrend_side


    signal_dict = get_signal_dict('supertrend', objective, timeframe, config_params)
    atr_range = signal_dict['atr_range']
    multiplier = signal_dict['multiplier']

    temp_df = ohlcv_df.copy()

    atr_list = cal_atr(ohlcv_df, atr_range)
    temp_df['atr'] = atr_list
    temp_df = temp_df.dropna().reset_index(drop=True)
    
    supertrend_list = []
    supertrend_side_list = []
    final_upperband_list = []
    final_lowerband_list = []

    for i in range(len(temp_df)):
        basic_upperband, basic_lowerband = cal_basic_band(temp_df, i, multiplier)
        final_upperband, final_lowerband = cal_final_band(temp_df, i, basic_upperband, basic_lowerband, final_upperband_list, final_lowerband_list)
        
        final_upperband_list.append(final_upperband)
        final_lowerband_list.append(final_lowerband)
            
        supertrend, supertrend_side = cal_supertrend(temp_df, i, final_upperband, final_lowerband, supertrend_side_list)
        
        supertrend_side_list.append(supertrend_side)
        supertrend_list.append(supertrend)

    ohlcv_df['supertrend'] = ([None] * (atr_range - 1)) + supertrend_list
    ohlcv_df['supertrend_side'] = ([None] * (atr_range - 1)) + supertrend_side_list
    
    return ohlcv_df


def add_wt(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('wt', objective, timeframe, config_params)
    channel_range = signal_dict['channel_range']
    average_range = signal_dict['average_range']
    
    temp_df = ohlcv_df.copy()
    
    temp_df['average_price'] = (temp_df['high'] + temp_df['low'] + temp_df['close']) / 3
    temp_df['esa'] = temp_df['average_price'].ewm(span=channel_range).mean()
    temp_df['dd'] = abs(temp_df['average_price'] - temp_df['esa'])
    temp_df['d'] = temp_df['dd'].ewm(span=channel_range).mean()
    temp_df['ci'] = (temp_df['average_price'] - temp_df['esa']) / (0.015 * temp_df['d'])
    
    wt_list = temp_df['ci'].ewm(span=average_range).mean()
    ohlcv_df['wt'] = wt_list
    
    return ohlcv_df


def add_rsi(objective, ohlcv_df, timeframe, config_params):
    signal_dict = get_signal_dict('rsi', objective, timeframe, config_params)
    windows = signal_dict['average_range']

    temp_df = ohlcv_df.copy()
    
    temp_df['diff'] = temp_df['close'].diff(1)
    temp_df['gain'] = temp_df['diff'].clip(lower=0)
    temp_df['loss'] = temp_df['diff'].clip(upper=0).abs()

    temp_df['avg_gain'] = temp_df['gain'].rolling(window=windows, min_periods=windows).mean()[:windows+1]
    temp_df['avg_loss'] = temp_df['loss'].rolling(window=windows, min_periods=windows).mean()[:windows+1]

    for i, _ in enumerate(temp_df.loc[windows + 1:, 'avg_gain']):
        temp_df.loc[i + windows + 1, 'avg_gain'] = (temp_df.loc[i + windows, 'avg_gain'] * (windows - 1) + temp_df.loc[i + windows + 1, 'gain']) / windows

    for i, _ in enumerate(temp_df.loc[windows + 1:, 'avg_loss']):
        temp_df.loc[i + windows + 1, 'avg_loss'] = (temp_df.loc[i + windows, 'avg_loss'] * (windows - 1) + temp_df.loc[i + windows + 1, 'loss']) / windows

    temp_df['rs'] = temp_df['avg_gain'] / temp_df['avg_loss']
    
    rsi_list = 100 - (100 / (1.0 + temp_df['rs']))
    ohlcv_df['rsi'] = rsi_list
  
    return ohlcv_df


def add_donchian(objective, ohlcv_df, timeframe, config_params):
    def get_donchian_side(ohlcv_df):
        if ohlcv_df['close'] > ohlcv_df['max_high']:
            signal_side = 'buy'
        elif ohlcv_df['close'] < ohlcv_df['min_low']:
            signal_side = 'sell'
        else:
            signal_side = np.nan

        return signal_side
    
    
    signal_dict = get_signal_dict('donchian', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    temp_df = ohlcv_df.copy()

    max_high_list = temp_df['high'].rolling(window=windows).max()
    min_low_list = temp_df['low'].rolling(window=windows).min()
    
    temp_df['max_high'] = max_high_list
    temp_df['min_low'] = min_low_list
    
    temp_df['max_high'] = temp_df['max_high'].shift(periods=1)
    temp_df['min_low'] = temp_df['min_low'].shift(periods=1)
    temp_df['donchian_side'] = temp_df.apply(get_donchian_side, axis=1)
    
    donchian_list = temp_df['donchian_side'].fillna(method='ffill')
    ohlcv_df['donchian_side'] = donchian_list
    
    return ohlcv_df


def add_hull(objective, ohlcv_df, timeframe, config_params):
    def cal_wma(series):
        weight_list = list(range(1, len(series) + 1))
        weighted_average_list = [i * j for i, j in zip(series, weight_list)]

        wma = sum(weighted_average_list) / sum(weight_list)

        return wma
    
    
    def add_wma(df, input_col, windows):
        wma_list = [None] * (windows - 1)

        for i in range(len(df) - (windows - 1)):
            close_series = df.loc[i:i + (windows - 1), input_col]
            wma = cal_wma(close_series)
            wma_list.append(wma)

        return wma_list
    
    
    def get_hull_side(ohlcv_df):
        if ohlcv_df['hull'] > ohlcv_df['hull_prev']:
            signal_side = 'buy'
        else:
            signal_side = 'sell'

        return signal_side
    
    
    signal_dict = get_signal_dict('hull', objective, timeframe, config_params)
    windows = signal_dict['windows']
    
    temp_df = ohlcv_df.copy()

    temp_df['hwma'] = add_wma(temp_df, 'close', int(round(windows / 2)))
    temp_df['wma'] = add_wma(temp_df, 'close', windows)
    temp_df['twma'] = (2 * temp_df['hwma']) - temp_df['wma']
    hull_list = add_wma(temp_df, 'twma', int(round(windows ** (1 / 2))))
    
    temp_df['hull'] = hull_list
    temp_df['hull_prev'] = temp_df['hull'].shift(periods=2)
    hull_side_list = temp_df.apply(get_hull_side, axis=1)
    
    ohlcv_df['hull'] = hull_list
    ohlcv_df['hull_side'] = hull_side_list
    
    return ohlcv_df


def add_action_signal(ohlcv_df_dict, func_add_dict, config_params):
    for symbol_type in ['base', 'lead']:
        for timeframe in ohlcv_df_dict[symbol_type]:
            for symbol in ohlcv_df_dict[symbol_type][timeframe]:
                ohlcv_df = ohlcv_df_dict[symbol_type][timeframe][symbol]
                
                for objective in ['open', 'close']:
                    if timeframe in config_params[symbol_type][objective]:
                        for signal in config_params[symbol_type][objective][timeframe]:
                            if signal not in ohlcv_df.columns:
                                print(f"{symbol_type} add {signal} to {symbol} {timeframe}")
                                ohlcv_df = func_add_dict[signal](objective, ohlcv_df, timeframe, config_params)

                ohlcv_df_dict[symbol_type][timeframe][symbol] = ohlcv_df

    return ohlcv_df_dict


def add_stop_signal(ohlcv_df_dict, func_add_dict, config_params):
    for objective in ['tp', 'sl']:
        if (config_params[objective]['signal'] != None):
            for symbol in ohlcv_df_dict['base'][config_params[objective]['signal']['timeframe']]:
                ohlcv_df = ohlcv_df_dict['base'][config_params[objective]['signal']['timeframe']][symbol]
                signal = list(config_params[objective]['signal']['signal'])[0]
                timeframe = config_params[objective]['signal']['timeframe']

                if signal not in ohlcv_df.columns:
                    print(f"{objective} add {signal} to {symbol} {timeframe}")
                    ohlcv_df = func_add_dict[signal](objective, ohlcv_df, timeframe, config_params)
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

    ohlcv_df_dict = add_action_signal(ohlcv_df_dict, func_add_dict, config_params)
    ohlcv_df_dict = add_stop_signal(ohlcv_df_dict, func_add_dict, config_params)
    ohlcv_df_dict = filter_start_time(start_date, ohlcv_df_dict, interval_dict)

    return ohlcv_df_dict