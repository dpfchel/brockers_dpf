from src.brockers_dpf.framework.internal.kafka.subscriber import Subscriber

class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"


class RegisterEventsSubscriberError(Subscriber):
    topic: str = "register-events-errors"