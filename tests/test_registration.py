import json
import time
import uuid

from src.brockers_dpf.framework.internal.kafka.consumer import Consumer
from src.brockers_dpf.framework.internal.http.account import AccountApi
from src.brockers_dpf.framework.internal.http.mail import MailApi
from src.brockers_dpf.framework.internal.kafka.producer import Producer
from kafka import KafkaConsumer


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
            confirmation_link_url = body_json['ConfirmationLinkUrl']
            uuid_part = confirmation_link_url.split('/')[-1]
            print(str(uuid_part))
            response = account.activate_user(token=uuid_part)          #активация учетной записи
            assert response.status_code == 200, "Email found, but Error activate" + str(response.content)
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")




def test_success_registration_with_kafka_producer_consumer(
        kafka_consumer: Consumer,
        kafka_producer: Producer
) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
    }

    kafka_producer.send("register-events", message)

    for i in range(10):
        message = kafka_consumer.get_message()
        try:                                                # Падали на:  # ConsumerRecord(topic='register-events', partition=0, leader_epoch=0, offset=770, timestamp=1770579596298, timestamp_type=0, key=None, value={'input_data': {'login': 'd8176fe0613048a3a73a7efbff4c47a4', 'email': 'd8176fe0613048a3a73a7efbff4c47a4@mail.ru', 'password': '123123123'}, 'error_message': {'type': 'https://tools.ietf.org/html/rfc7231#section-6.5.1', 'title': 'Validation failed', 'status': 400, 'traceId': '00-2bd2ede7c3e4dcf40c4b7a62ac23f448-839ff284720ea656-01', 'errors': {'Email': ['Invalid']}}, 'error_type': 'unknown'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=393, serialized_header_size=-1)
            login_from_message = message.value["login"]
        except Exception:
            try:
                login_from_message = message.value["value"]["input_data"]["login"]
            except Exception:
                login_from_message = ''

        if login_from_message == base:
            break
    else:
        raise AssertionError("Email not found")



