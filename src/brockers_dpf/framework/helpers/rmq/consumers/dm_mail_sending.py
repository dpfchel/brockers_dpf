import json
import time

from brockers_dpf.framework.internal.rmq.consumer import ConsumerRmq


class DmMailSending(ConsumerRmq):
    exchange = "dm.mail.sending"
    routing_key = '#'


    def find_message(
            self,
            login: str,
            timeout: float = 20.0
    ) -> None:

        start_time = time.time()
        success_flag = 0
        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)

            message_body_login =''
            try:
                message_body_login = json.loads(message["body"])["Login"]
            except json.JSONDecodeError as e:
                print(f'Ошибка декодирования JSON: {e}')

            if message_body_login == login:
                success_flag = 1
                break

        if success_flag == 0:
            raise AssertionError(f"Message for rmq: {self.exchange} not found")