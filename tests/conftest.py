import pytest
from src.framework.internal.http.account import AccountApi
from src.framework.internal.http.mail import MailApi
from src.framework.internal.kafka.producer import Producer



@pytest.fixture(scope="session")
def account() -> AccountApi:
    return AccountApi()


@pytest.fixture(scope="session")
def mail() -> MailApi:
    return MailApi()


@pytest.fixture(scope="session")
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer