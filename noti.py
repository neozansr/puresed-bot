import requests
import json


def line_send(message):
    payload = {'message':message}
    send_message = get_message(payload)
    
    return send_message


def get_message(payload):
    url = 'https://notify-api.line.me/api/notify'
    
    with open('_keys/line_token.json') as token_file:
        token_dict = json.load(token_file)
    token = token_dict['tradebot']
    
    headers = {'Authorization':'Bearer ' + token}
    
    return requests.post(url, headers = headers , data = payload)