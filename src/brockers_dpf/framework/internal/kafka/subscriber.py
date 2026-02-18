from abc import ABC, abstractmethod
from kafka.consumer.fetcher import ConsumerRecord
import queue


class Subscriber(ABC):
    def __init__(self):
        self._messages: queue.Queue = queue.Queue()

    @property
    @abstractmethod
    def topic(self) -> str:
        pass


    def handle_message(self, record: ConsumerRecord) -> None:
        self._messages.put(record)


    def get_message(self, timeout: int = 90):
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            if timeout < 90:                # для /tests/conftest.py - clear_topic_register_events_errors
                return None
            else:
                raise AssertionError(f"No messages from topic: {self.topic}, within timeout: {timeout}")


    def get_message_find(self, find_str: str = None):
        assert find_str != None, "Задайте строку для поиска в сообщении"
        while True:                                                     # когда закончится очередь - упадем по таймауту
            if find_str != None:
                message = self.get_message()
                if find_str in str(message):
                    return message
