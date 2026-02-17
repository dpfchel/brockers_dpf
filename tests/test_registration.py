import json
import time
import uuid
import pytest
import pika

from brockers_dpf.framework.internal.rmq.publisher import RmqPublisher
from src.brockers_dpf.framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from src.brockers_dpf.framework.helpers.kafka.consumers.register_events_errors import RegisterEventsSubscriberError

from src.brockers_dpf.framework.internal.http.account import AccountApi
from src.brockers_dpf.framework.internal.http.mail import MailApi
from src.brockers_dpf.framework.internal.kafka.producer import Producer


@pytest.fixture
def register_message() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123123",
    }



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



def test_success_registration_with_kafka_producer(
        register_message: dict[str, str],
        mail: MailApi,
        kafka_producer: Producer
) -> None:
    login = register_message["login"]

    kafka_producer.send("register-events", register_message)
    for _ in range(10):
        response = mail.find_message(query=login)
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
        register_events_subscriber: RegisterEventsSubscriber,
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
        message = register_events_subscriber.get_message()
        login_from_message = message.value["login"]
        if login_from_message == base:
            break
    else:
        raise AssertionError("Email not found")



# 12- 1
def test_success_registration_12(
        register_events_subscriber: RegisterEventsSubscriber,
        register_message: dict[str, str],
        account: AccountApi,
        mail: MailApi,
) -> None:

    login = register_message["login"]
    account.register_user(**register_message)

    message = register_events_subscriber.get_message()
    login_from_message = message.value["login"]

    assert login_from_message == login

    for _ in range(10):
        response = mail.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")



# Задания - вторая неделя    # clear_topic_register_events_errors,
def test_invalid_data_to_error_type_validation(
        register_events_subscriber: RegisterEventsSubscriber,
        register_events_subscriber_error: RegisterEventsSubscriberError,
        account: AccountApi,
) -> None:
    """  Задание 2.1
    - Запускаем асинхронную регистрацию с невалидными данными через Register API
    - Проверям, что сообщение попало в топик register-events
    - Проверяем, что сообщение попало в топик register-events-error с типом ошибки validation
    """

    message_invalid = {
        "login": "1",
        "email": "1@1",
        "password": "1",
    }

    account.register_user(**message_invalid)
    # topic: "register-events"
    message = register_events_subscriber.get_message()
    if message.value["login"] != message_invalid["login"] or message.value["email"] != message_invalid["email"]:
        raise AssertionError("В топике register-events не совпал email ")

    #topic: "register-events-errors"

    message_from_error = register_events_subscriber_error.get_message_find(message_invalid["email"])
    message_from_error_value = message_from_error.value
    message_from_error_value_input_data = message_from_error_value["input_data"]
    login = message_from_error_value_input_data["login"]
    email = message_from_error_value_input_data["email"]
    error_type = message_from_error_value["error_type"]

    if login != message_invalid["login"] or email != message_invalid["email"]:
        raise AssertionError("В топике register-events-errors: не совпали логин или емайл ")
    assert error_type == 'validation', "В топике register-events-errors: error_type не validation "





def test_invalid_data_with_error_type_unknown_to_error_type_validation(
        register_events_subscriber_error: RegisterEventsSubscriberError,
        account: AccountApi,
        kafka_producer: Producer,
) -> None:
    """ Задание 2.2
     - Запушить в топик register-events-error  сообщение с ошибкой валидации, но изменить тип на unknown,
     - проверить, что сообщение повторно попадет в топик  register-events-error , но уже с типом ошибки "validation"
     - т.е. Принимаем 2 сообщения: первое с типом ошибки "unknown", второе с типом ошибки "validation"
    """
    message_incorrect = {
        'input_data': {
            'login': '2',
            'email': '2@2',
            'password': '2'
        },
        'error_message': {
            'type': 'https://tools.ietf.org/html/rfc7231#section-6.5.1',
            'title': 'Validation failed',
            'status': 400,
            'traceId': '00-71e7897b8c9979e51ecebf7099ce41c0-3e1a02fb9e5eb0a0-01',
            'errors': {
                'Login': ['Short'],
                'Password': ['Short']
            }
        },
        'error_type': 'unknown'
    }

    # Находим переменные из тестовых данных
    message_incorrect_value = message_incorrect
    message_incorrect_value_input_data = message_incorrect_value["input_data"]
    login_message_incorrect = message_incorrect_value_input_data["login"]
    email_message_incorrect = message_incorrect_value_input_data["email"]
    error_type_message_incorrect = message_incorrect_value["error_type"]
    assert error_type_message_incorrect == 'unknown', "Ошибка в тестовых данных, error_type не unknown"

    kafka_producer.send("register-events-errors", message_incorrect)

    #topic: "register-events-errors"
    flag_is_ok = 0
    case = 0              # количество сообщений с тестовыми логином и емайлом
    for i in range(2):
        message_from_topic_error = register_events_subscriber_error.get_message_find(email_message_incorrect)
        message_from_topic_error_value = message_from_topic_error.value
        message_from_topic_error_value_input_data = message_from_topic_error_value["input_data"]
        login = message_from_topic_error_value_input_data["login"]
        email = message_from_topic_error_value_input_data["email"]
        error_type = message_from_topic_error_value["error_type"]

        if login != login_message_incorrect or email != email_message_incorrect:
            raise AssertionError("В топике register-events-errors: не совпали логин или емайл ")

        if i == 0:
            assert error_type == 'unknown', f"В register-events-errors на шаге 0: error_type не unknown, step {i}, {error_type}"
        if i == 1:
            assert error_type == 'validation', f"В register-events-errors на шаге 1: error_type не validation, step {i}, {error_type}"



def test_rmq(rmq_publisher: RmqPublisher) -> None:
    address = f"{uuid.uuid4().hex}@mail.ru"
    message = {
        "address": address,
        "subject": "Test message",
        "body":  "Test message",
    }
    #rmq_publisher.publish("dm.mail.sending", message=message)
    print(rmq_publisher)
    rmq_publisher.publish("dm.mail.sending", message)
