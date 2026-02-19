from typing import Generator

import pytest

from brockers_dpf.framework.helpers.rmq.consumers.dm_mail_sending import DmMailSending
from src.brockers_dpf.framework.internal.rmq.publisher import RmqPublisher
from src.brockers_dpf.framework.helpers.kafka.consumers.register_events_errors import RegisterEventsSubscriberError
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


# Вычитаем все сообщения с топика, если timeout<90 get_message вернет None
@pytest.fixture()
def clear_topic_register_events_errors(register_events_subscriber_error) -> None:
    result = 1
    while result != None:
        result = register_events_subscriber_error.get_message(timeout=2)


@pytest.fixture(scope="session")
def rmq_publisher() -> Generator[RmqPublisher]:
    with RmqPublisher() as publisher:
        yield publisher

@pytest.fixture(scope="session", autouse=True)
def rmq_dm_mail_sending_consumer(
        register_events_subscriber: RegisterEventsSubscriber,
) -> Generator[DmMailSending]:
    with DmMailSending() as consumer:
        yield consumer