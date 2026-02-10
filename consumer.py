import json
import threading
import time

from kafka import KafkaConsumer

#class
def consume_message():
    consumer = KafkaConsumer(
        "register-events",
        bootstrap_servers=["185.185.143.231:9092"],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    try:
        for message in consumer:
            try:  # Падали на:  # ConsumerRecord(topic='register-events', partition=0, leader_epoch=0, offset=770, timestamp=1770579596298, timestamp_type=0, key=None, value={'input_data': {'login': 'd8176fe0613048a3a73a7efbff4c47a4', 'email': 'd8176fe0613048a3a73a7efbff4c47a4@mail.ru', 'password': '123123123'}, 'error_message': {'type': 'https://tools.ietf.org/html/rfc7231#section-6.5.1', 'title': 'Validation failed', 'status': 400, 'traceId': '00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01', 'errors': {'Email': ['Invalid']}}, 'error_type': 'unknown'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=393, serialized_header_size=-1)
                login_from_message = message.value["login"]
            except Exception as er:
                login_from_message = f"{er}"

            print(login_from_message)
    except Exception as e:
        print( f"Error {e}")
    finally:
        consumer.close()

thread = threading.Thread(target=consume_message, daemon=True)
thread.start()
time.sleep(5)
#consume_message()
print("STOP")
