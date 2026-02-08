import json
import time
import uuid
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


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
    account.register_user(login=base, email=f"{base}@mail.ru", password="123123123")
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")



def test_success_registration_with_kafka_producer(mail: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
    }
    kafka_producer.send("register-events", message)
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")



# Задание ( 1 неделя)
def test_register_events_error_consumer(account: AccountApi, mail: MailApi, kafka_producer: Producer) -> None:
    base = "dpf_" + str(uuid.uuid4().hex)
    # Некорректное сообщение, с уникальными login email
    message_incorrect = {
        "input_data": {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
      },
      "error_message": {
        "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
        "title": "Validation failed",
        "status": 400,
        "traceId": "00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01",
        "errors": {"Email": ["Invalid"]}
      },
      "error_type": "unknown"
    }
    kafka_producer.send("register-events-errors", message_incorrect)
    # ОР: т.к. "error_type": "unknown" - будут повторные попытки создать учетную запись и письмо будет создано
    for _ in range(10):
        response = mail.find_message(query=base)
        if response.json()["total"] > 0:
            body_str = response.json()['items'][0]['Content']['Body']
            body_json = json.loads(body_str)
            ConfirmationLinkUrl = body_json['ConfirmationLinkUrl']
            uuid_part = ConfirmationLinkUrl.split('/')[-1]
            print(str(uuid_part))
            response = account.activate_user(token=uuid_part)          #активация учетной записи
            assert response.status_code == 200, "Email found, but Error activate" + str(response.content)
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")

