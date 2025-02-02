import pandas as pd
import json
from kafka import KafkaProducer

df = pd.read_csv('online.csv')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("OK")
for index, row in df.iterrows():
    record = row.drop('Diabetes_binary').to_dict()
    producer.send('diabetes_data', value=record)
    print(f'Sent record: {record}')

producer.flush()
producer.close()

