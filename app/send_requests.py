import requests


msg_sample = {'msg': 'this is my message'}

r = requests.post('http://127.0.0.1:5000/invocations',
                  data=msg_sample)

print(r.json()['msg'])