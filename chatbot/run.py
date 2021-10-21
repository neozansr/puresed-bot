import telebot
import time
import os
import sys

home_path = '../'
src_path = home_path + 'src/'
sys.path.append(os.path.abspath(src_path))

from func_get import get_json
from func_chat import get_balance_text, get_yield_text, get_rebalance_text, get_grid_text, get_technical_text


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
cash_flow_path = home_path + 'cash_flow/{}.csv'


bot = telebot.TeleBot(token)
x = bot.get_me()
print(x)


@bot.message_handler(commands=['start', 'help', 'h'])
def send_help(message):
    text = "type /balance_[account] to get balance info"
    text += "\ntype /yield_[account] to get yield info"
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
    bot_dict = {
        'bot_rebalance':'rebalance',
        'bot_grid':'grid',
        'bot_technical':'technical',
        'hold':'hold'
        }
    
    text = get_balance_text(home_path, bot_dict, config_system_path, config_params_path, transfer_path, profit_df_path, transactions_df_path, position_path, cash_flow_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['yield_dev'])
def send_yield(message):
    bot_dict = {
        'bot_rebalance':'rebalance',
        'bot_grid':'grid',
        'bot_technical':'technical',
        'hold':'hold'
        }
    
    text = get_yield_text(home_path, bot_dict, config_params_path, profit_df_path, transactions_df_path, position_path, transfer_path, cash_flow_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_rebalance'])
def send_bot_rebalance(message):
    bot_name = 'bot_rebalance'
    bot_type = 'rebalance'
    
    text = get_rebalance_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, transfer_path, profit_df_path, cash_flow_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_grid'])
def send_bot_grid(message):
    bot_name = 'bot_grid'
    bot_type = 'grid'

    text = get_grid_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, transfer_path, open_orders_df_path, transactions_df_path, cash_flow_path)
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_technical'])
def send_bot_technical(message):
    bot_name = 'bot_technical'
    bot_type = 'technical'
    
    text = get_technical_text(home_path, bot_name, bot_type, config_system_path, config_params_path, last_loop_path, position_path, transfer_path, cash_flow_path)
    bot.send_message(message.chat.id, text)


while True:
    try:
        bot.polling()
    except Exception:
        time.sleep(15)