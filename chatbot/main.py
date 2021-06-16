import telebot
import ccxt
import requests
import time
import json


from func_get import get_grid_text, get_rebalance_text

with open('../../_keys/bot_token.json') as token_file:
    token_dict = json.load(token_file)
token = token_dict['telegram']

config_system_path = 'config_system.json'
config_params_path = 'config_params.json'
last_loop_path = 'last_loop.json'
open_orders_df_path = 'open_orders.csv'
profit_df_path = 'profit.csv'
cash_flow_df_path = '../cash_flow/{}.csv'


bot = telebot.TeleBot(token)
x = bot.get_me()
print(x)


@bot.message_handler(commands = ['start', 'help', 'h'])
def send_help(message):
    text = 'type /[bot_name] to get info'
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands = ['bot_rebalance'])
def send_bot_rebalance(message):
    bot_name = 'bot_rebalance'
    bot_type = 'rebalance'
    sub_path = '../{}/'.format(bot_name)

    text = '{}\n{}\n'.format(bot_name.title(), bot_type.title())
    text = get_rebalance_text(text, bot_type, sub_path, config_system_path, config_params_path, profit_df_path)
    
    bot.send_message(message.chat.id, text)


@bot.message_handler(commands = ['bot_grid'])
def send_bot_grid(message):
    bot_name = 'bot_grid'
    bot_type = 'grid'
    sub_path = '../{}/'.format(bot_name)

    text = '{}\n{}\n'.format(bot_name.title(), bot_type.title())
    text = get_grid_text(text, bot_name, bot_type, sub_path, config_system_path, config_params_path, open_orders_df_path, cash_flow_df_path)
    
    bot.send_message(message.chat.id, text)


while True:
    try:
        bot.polling()
    except Exception:
        time.sleep(15)