#!python3
import time
import json
import requests
import pandas
import connection

from hdfs import InsecureClient
from datetime import datetime

if __name__=="__main__":
    conf_hadoop = connection.param_config("hadoop")["ip"]
    client = InsecureClient(conf_hadoop)
    link = "https://raw.githubusercontent.com/rahmat14/DigitalSkola/main/TR_UserStatus.json"
    req = requests.get(link)
    data = req.json()['data']
    list_data = []
    for dic in data:
        dict_data = {}
        dict_data['UserID']=dic['User']
        dict_data["FirstLogin"]=dic['context'][0]['first']
        dict_data["LastLogin"]=dic['context'][0]['last']

        list_data.append(dict_data)
    time = datetime.now().strftime("%Y%m%d")
    df= pandas.DataFrame(list_data)
    with client.write(f'/Project5/{time}/api_{time}.csv', encoding='utf-8') as writer:
            df.to_csv(writer, index=False)
    print('API Success Ingest....')