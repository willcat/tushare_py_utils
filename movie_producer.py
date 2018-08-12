import tushare as ts
from kafka import KafkaProducer
import json
df = ts.realtime_boxoffice()
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for idx in df.index:
    message = str(df.loc[idx].values[0:-1])
    producer.send('foobar', bytes(message,encoding='utf-8'))
    print(message)
