import telebot
import time
import os
import sys

home_path = '../'
src_path = home_path + 'src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_json
from func_chat import get_balance_text, get_cash_flow_text, get_rebalance_text, get_grid_text, get_technical_text


token_path = home_path + '../_keys/bot_token.json'
token_dict = get_json(token_path)
token = token_dict['telegram']

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
position_path = 'position.json'
transfer_path = 'transfer.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
profit_df_path = 'profit.csv'
cash_flow_df_path = 'cash_flow.csv'


bot = telebot.TeleBot(token)
x = bot.get_me()
print(x)


@bot.message_handler(commands=['start', 'help', 'h'])
def send_help(message):
    text = "type /balance_[account] to get balance info"
    text += "\ntype /cash_flow_[account] to get avaialble cash flow info"
    text += "\ntype /[bot_name] to get bot status"

    text += "\navaialble [account]:"
    text += "\n   dev"
    
    text += "\navaialble [bot_name]:"
    text += "\n   bot_rebalance"
    text += "\n   bot_grid"
    text += "\n   bot_technical"
    
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['balance_dev'])
def send_balance(message):
    bot_list = [
        'bot_rebalance',
        'bot_grid',
        'bot_technical',
        'hold'
    ]
    
    text = get_balance_text(bot_list, config_system_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['cash_flow_dev'])
def send_cash_flow(message):
    bot_list = [
        'bot_rebalance',
        'bot_grid'
    ]
    
    text = get_cash_flow_text(home_path, bot_list, transfer_path, cash_flow_df_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_rebalance'])
def send_bot_rebalance(message):
    bot_name = 'bot_rebalance'
    bot_type = 'rebalance'
    
    text = get_rebalance_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_df_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_grid'])
def send_bot_grid(message):
    bot_name = 'bot_grid'
    bot_type = 'grid'

    text = get_grid_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_df_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_technical'])
def send_bot_technical(message):
    bot_name = 'bot_technical'
    bot_type = 'technical'
    
    text = get_technical_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, position_path, profit_df_path)
    bot.send_message(message.chat.id, text)


while True:
    try:
        bot.polling()
    except Exception:
        config_system = get_json(config_system_path)
        time.sleep(config_system['idle_loop'])