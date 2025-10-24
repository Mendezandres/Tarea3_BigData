from kafka import KafkaProducer
import pandas as pd
import time, json

# Configurar productor
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Cargar dataset
df = pd.read_csv("/home/vboxuser/creditcard.csv")

# Enviar datos uno por uno
for _, row in df.iterrows():
    data = {"Time": row["Time"], "Amount": row["Amount"], "Class": row["Class"]}
    producer.send("fraud_topic", value=data)
    print(f"Enviado: {data}")   # muestra lo que se envía
    time.sleep(0.5)

producer.flush()  # Asegura que todo se envíe antes de cerrar
