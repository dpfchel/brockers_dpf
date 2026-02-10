import json
import threading
import time
import queue
from queue import Queue

from kafka import KafkaConsumer

q = queue.Queue()
running = threading.Event()

#class
def consume_message():
    consumer = KafkaConsumer(
        "register-events",
        bootstrap_servers=["185.185.143.231:9092"],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    try:
        while running.is_set():
            messages = consumer.poll(timeout_ms=1000, max_records=10)
            for topic_partition, records in messages.items():
                for record in records:
                    print(record)
                    q.put(record)
        print("Stop consuming")
    except Exception as e:
        print( f"Error {e}")
    finally:
        consumer.close()

running.set()                                                       # установили флаг
thread = threading.Thread(target=consume_message, daemon=True)       # запустили поток
thread.start()
time.sleep(1)


def get_message(timeout=90):
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        raise AssertionError("Queue is empty")

print(q.qsize())
print(get_message())
time.sleep(2)
print("STOP")
print(q.qsize())

running.clear()                                                     # сбросили флаг