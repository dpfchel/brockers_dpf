from src.brockers_dpf.framework.internal.kafka.subscriber import Subscriber

class RegisterEventsSubscriberError(Subscriber):
    topic: str = "register-events-errors"