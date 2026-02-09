import json
from kafka import KafkaConsumer



#class
consumer = KafkaConsumer(
    "register-events",
    bootstrap_servers=["185.185.143.231:9092"],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for message in consumer:
    print(message)

consumer.close()