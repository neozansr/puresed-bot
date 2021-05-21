import telebot
import json

with open('_keys/bot_token.json') as token_file:
    token_dict = json.load(token_file)
token = token_dict['telegram']

bot = telebot.TeleBot(token)
x = bot.get_me()
print(x)


@bot.message_handler(commands = ['start', 'help', 'h'])
def send_help(message):
    text = '/b: get current balance\n' \
           '/ob: get max open buy oders\n' \
           '/os: get min open sell orders'
    bot.send_message(message.chat.id, text)


bot.polling()