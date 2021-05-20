import telebot
import json

with open('_keys/bot_token.json') as token_file:
    token_dict = json.load(token_file)
token = token_dict['telegram']

bot = telebot.TeleBot(token)
x = bot.get_me()
print(x)

@bot.message_handler(commands = ['start', 'help', 'h'])
def send_welcome(message):
    text = '/balance /b: see current balance\n' \
           'new commands to come'
    bot.send_message(message.chat.id, message)

bot.polling()