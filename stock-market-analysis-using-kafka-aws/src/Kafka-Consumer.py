
import pandas as pd 
from kafka import KafkaConsumer
from time import sleep
from json import loads
import json
from s3fs import S3FileSystem

consumer = KafkaConsumer('kafkaTopic',
                         bootstrap_servers=['3.67.79.44:9092'],  #VM's public IP
                        value_deserializer=lambda x:loads(x.decode('utf-8')))

s3 = S3FileSystem()
    
for count, i in enumerate(consumer):
    with s3.open("s3://de-projects-bucket-shubh/stock-market-using-kafka-aws/stock_market_{}.json".format(count), 'w') as file:
        json.dump(i.value, file)