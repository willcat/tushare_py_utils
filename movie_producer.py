import tushare as ts
from kafka import KafkaProducer
import json
df = ts.realtime_boxoffice()
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for idx in df.index:
    message = df.loc[idx].values[0:-1].tolist()
    producer.send('foobar', bytes(json.dumps(message),encoding='utf-8'))
    print(message)
