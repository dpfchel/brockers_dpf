import time
import uuid

from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi


def test_failed_registration(account: AccountApi, mail: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", email=expected_mail, password="string")
    for _ in range(10):
        response = mail.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email found")
        time.sleep(1)



def test_success_registration(account: AccountApi, mail: MailApi) -> None:
    base = uuid.uuid4().hex
    response_account = account.register_user(login=str(base), email=f"{base}@mail.ru", password="123123123")
    for _ in range(10):
        response_mail = mail.find_message(query=str(base))
        if response_mail.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")