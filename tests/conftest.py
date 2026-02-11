from typing import Generator

import pytest

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



@pytest.fixture(scope="session", autouse=True)
def kafka_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
) -> Generator[Consumer]:
    with Consumer(subscribers=[register_events_subscriber]) as consumer:
        yield consumer