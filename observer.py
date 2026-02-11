from typing import Protocol



# Гуси(ути)ная типизация - если класс имеет метод "update", то это Observer ( так работает Protocol)
class Observer(Protocol):
    def update(self, message: str):
        pass

    
# Создаем одно соединение, слушаем много топиков и всех подписчиков уведломляем
class Subject:
    def __init__(self):
        self._subscribers: list = []

    def register(self, observer: 'Observer'):
        self._subscribers.append(observer)


    def notify(self, message: str):
        for observer in self._subscribers:
            observer.update(message)



class RegisterEventsSubscriber:
    def __init__(self):
        self.messages = []


    def update(self, message):
        self.messages.append({"register-events": message})


    def get_messages(self):
        return self.messages



class AnotherTopicSubscriber:
    def __init__(self):
        self.messages = []


    def update(self, message):
        self.messages.append({"another-topic": message})


    def get_messages(self):
        return self.messages


if __name__ == "__main__":
    subject = Subject()
    s1 = AnotherTopicSubscriber()
    s2 = RegisterEventsSubscriber()
    subject.register(s1)
    subject.register(s2)

    subject.notify("Hello")

    print(s1.get_messages())
    print(s2.get_messages())