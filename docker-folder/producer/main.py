#!/usr/bin/python3

import os
import time

import kafka.errors
import pandas as pd
from kafka import KafkaProducer

TOPIC = os.environ["TOPIC"]
KAFKA_BROKER = os.environ["KAFKA_BROKER"]

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


data = pd.read_csv("./proba.csv")
for index, row in data.iterrows():
    print(TOPIC, str(row))
    data_str = f"{row.person_id} {row.x_min} {row.y_min} {row.x_max} {row.y_max} {row.room_name}"  # noqa E501
    producer.send(
        TOPIC,
        key=bytes(str(index), "utf-8"),
        value=bytes(data_str, "utf-8"),
    )
    if index % 3 == 0:
        time.sleep(1)
