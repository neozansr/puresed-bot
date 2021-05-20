def cal_buy_amount(current_value, fix_value, latest_price):
    diff_value = fix_value - current_value
    buy_amount = diff_value / latest_price

    return buy_amount


def cal_sell_amount(current_value, fix_value, latest_price):
    diff_value = current_value - fix_value
    sell_amount = diff_value / latest_price

    return sell_amount