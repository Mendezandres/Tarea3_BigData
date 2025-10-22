from kafka import KafkaProducer
import pandas as pd
import time, json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

df = pd.read_csv("/home/vboxuser/creditcard.csv")

for _, row in df.iterrows():
    data = {"Time": row["Time"], "Amount": row["Amount"], "Class": row["Class"]}
    producer.send("fraud_topic", value=data)
    time.sleep(0.5)
