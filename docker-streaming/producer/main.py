#!/usr/bin/python3

import os
import time

import kafka.errors
import pandas as pd
from kafka import KafkaProducer

SUBREDDIT = os.environ["SUBREDDIT"]
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "subreddit-" + SUBREDDIT

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


data = pd.read_csv("./train.csv")
for index, row in data.iterrows():
    if index % 2 == 0 and TOPIC == "subreddit-worldnews":
        continue
    elif index % 2 != 0 and TOPIC == "subreddit-politics":
        continue
    print("SALJEM", TOPIC, index, row.text)
    producer.send(
        TOPIC, key=bytes(str(index), "utf-8"), value=bytes(row.text, "utf-8"),
    )
    time.sleep(1)
