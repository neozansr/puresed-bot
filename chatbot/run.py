import telebot
import time
import json
import os
import sys

src_path = '../src/'
sys.path.append(os.path.abspath(src_path))

from func_chat import get_grid_text, get_rebalance_text, get_technical_text

with open('../../_keys/bot_token.json') as token_file:
    token_dict = json.load(token_file)
token = token_dict['telegram']

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
open_orders_df_path = 'open_orders.csv'
transactions_df_path = 'transactions.csv'
profit_df_path = 'profit.csv'


bot = telebot.TeleBot(token)
x = bot.get_me()
print(x)


@bot.message_handler(commands=['start', 'help', 'h'])
def send_help(message):
    text = "type /[bot_name] to get info"
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_rebalance'])
def send_bot_rebalance(message):
    bot_name = 'bot_rebalance'
    bot_type = 'rebalance'
    sub_path = f'../{bot_name}/'

    text = f"{bot_name.title()}\n{bot_type.title()}\n"
    text = get_rebalance_text(text, sub_path, config_system_path, config_params_path, last_loop_path, profit_df_path)
    
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_grid'])
def send_bot_grid(message):
    bot_name = 'bot_grid'
    bot_type = 'grid'
    sub_path = f'../{bot_name}/'

    text = f"{bot_name.title()}\n{bot_type.title()}\n"
    text = get_grid_text(text, sub_path, config_system_path, config_params_path, last_loop_path, open_orders_df_path, transactions_df_path)
    
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands=['bot_technical'])
def send_bot_grid(message):
    bot_name = 'bot_technical'
    bot_type = 'technical'
    sub_path = f'../{bot_name}/'

    text = f"{bot_name.title()}\n{bot_type.title()}\n"
    text = get_technical_text(text, sub_path, config_system_path, config_params_path, last_loop_path, position_path)
    
    bot.send_message(message.chat.id, text)


while True:
    try:
        bot.polling()
    except Exception:
        time.sleep(15)