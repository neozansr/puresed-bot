import requests


def line_send(message):
    payload = {'message':message}
    send_message = get_message(payload)
    
    return send_message


def get_message(payload):
    url = 'https://notify-api.line.me/api/notify'
    token = 'LmZF8P5RxyEASoVL1Hinna1HUc04E7OBtC2nlYVl3Ub'
    headers = {'Authorization':'Bearer ' + token}
    
    return requests.post(url, headers = headers , data = payload)