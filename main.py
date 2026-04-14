import requests

base_url = "http://192.168.1.26/trade_demo/odata/standard.odata"
url = f"{base_url}/ExchangePlan_ДляODATA?$format=json"

response = requests.get(url,auth=('admin', 'admin'))

data = response.json()
exchange = data['value'][1]
exchange['Ref_Key']
exchange['SentNo']
exchange_guid = 'a9bc23c5-3689-11f1-926c-0800270bc6cb'

exchange_name = 'ДляODATA'
message_no=1

url = f"{base_url}/SelectChanges?DataExchangePoint='{base_url}/ExchangePlan_{exchange_name}(guid'{exchange_guid}')'&MessageNo={message_no}&$format=json"

response = requests.post(url,auth=('admin', 'admin'))
change_data = response.json()
change_data['value'][0]['Товары']

pass