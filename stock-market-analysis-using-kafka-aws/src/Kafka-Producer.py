
#install required libraries
pip install kafka-python
import pandas as pd 
from kafka import KafkaProducer, KafkaConsumer
from time import sleep
from json import dumps
import json


producer = KafkaProducer(bootstrap_servers=['3.67.79.44:9092'],
                        value_serializer=lambda x:
                        dumps(x).encode('utf-8'))


path = r"C:\Users\SHUBHAM\Desktop\Data-Engineering-End-to-End-Projects\Stock-Market-using-Apache-Kafka-AWS\indexProcessed.csv"
df = pd.read_csv(path)

df.head(5)

while True:
    dict_stock = df.sample(1).to_dict(orient='records')[0]
    producer.send('kafkaTopic', value = dict_stock)
    

