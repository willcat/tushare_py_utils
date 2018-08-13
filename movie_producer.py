import tushare as ts
from kafka import KafkaProducer
import json
import time,datetime
df = ts.realtime_boxoffice()
producer = KafkaProducer(bootstrap_servers='localhost:9092',batch_size=100)

def parseArray(value):
    arr = {}
    arr['boxOffice'] = value[0]
    arr['rank'] = value[1]
    arr['movieName'] = value[2]
    arr['boxPer'] = value[3]
    arr['movieDay'] = value[4]
    arr['sumBoxOffice'] = value[5]
    arr['timeStamp'] = int(time.mktime(time.strptime(value[6],"%Y-%m-%d %H:%M:%S")))
    return arr

for idx in df.index:
    message = df.loc[idx].values
    dics = parseArray(df.loc[idx].values)
    producer.send('foobar', bytes(json.dumps(dics),encoding='utf-8'))