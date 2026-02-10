from typing import Generator

import pytest

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
def kafka_consumer() -> Generator[Consumer]:
    with Consumer() as consumer:
        yield consumer