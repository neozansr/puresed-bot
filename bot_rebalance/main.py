from func_get import *
from func_order import *

keys_path = '../_keys/kucoin_0_keys.json'
config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'


def run_bot(idle_stage, keys_path = keys_path, config_system_path = config_system_path, config_params_path = config_params_path, open_orders_df_path = open_orders_df_path, transactions_df_path = transactions_df_path):
    bot_name = os.path.basename(os.getcwd())
    exchange = get_exchange(keys_path)
    open_orders_df = pd.read_csv(open_orders_df_path)
    transactions_df = pd.read_csv(transactions_df_path)
    symbol, fix_value, min_value, fee_percent = get_config_params(config_params_path)
    open_orders_df, transactions_df, cont_flag = check_open_orders(exchange, symbol, open_orders_df, transaction_df)
    time.sleep(idle_stage)

    if cont_flag == 1:
        latest_price = get_latest_price(exchange, symbol)
        rebalance_port(symbol, fix_value, min_value, latest_price)

    return open_orders_df, transactions_df


if __name__ == "__main__":
    run_flag = True
    while run_flag == True:
        run_flag, min_idle, max_idle = get_config_system(config_system_path)
        print('start loop')
        open_orders_df, transactions_df = run_bot(idle_stage)
        print('end loop')
        idle_loop = get_random(min_idle, max_idle)
        time.sleep(idle_loop)