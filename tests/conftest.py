from typing import Generator

import pytest

from brockers_dpf.framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriberError
from src.brockers_dpf.framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from src.brockers_dpf.framework.internal.kafka.consumer import Consumer
from src.brockers_dpf.framework.internal.http.account import AccountApi
from src.brockers_dpf.framework.internal.http.mail import MailApi
from src.brockers_dpf.framework.internal.kafka.producer import Producer



@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()


@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()


@pytest.fixture(scope="session")
def kafka_producer() -> Generator[Producer]:
    with Producer() as producer:
        yield producer


@pytest.fixture(scope="session")
def register_events_subscriber() -> RegisterEventsSubscriber:
    return  RegisterEventsSubscriber()

@pytest.fixture(scope="session")
def register_events_subscriber_error() -> RegisterEventsSubscriberError:
    return  RegisterEventsSubscriberError()


@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_subscriber_error: RegisterEventsSubscriberError
) -> Generator[Consumer]:
    with Consumer(subscribers=[
        register_events_subscriber,
        register_events_subscriber_error,
    ]) as consumer:
        yield consumer