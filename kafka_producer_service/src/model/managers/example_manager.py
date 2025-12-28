import threading
import time
from typing import Optional

from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from infrastructure.interfaces.izmq_client_manager import IZmqClientManager

from globals.consts.const_strings import ConstStrings
from globals.consts.consts import Consts
from globals.consts.const_colors import ConstColors
from globals.consts.logger_messages import LoggerMessages
from infrastructure.factories.logger_factory import LoggerFactory

from model.data_classes.zmq_request import Request


class ExampleManager(IExampleManager):
    _TAG_ZMQ_CLIENT = LoggerMessages.TAG_ZMQ_CLIENT
    _TAG_ZMQ_CLIENT_COLOR = ConstColors.CYAN

    _TAG_KAFKA = LoggerMessages.TAG_KAFKA
    _TAG_KAFKA_COLOR = ConstColors.MAGENTA

    def __init__(
        self,
        config_manager: IConfigManager,
        kafka_manager: IKafkaManager,
        zmq_client_manager: IZmqClientManager,
    ) -> None:
        super().__init__()
        self._config_manager = config_manager
        self._kafka_manager = kafka_manager
        self._zmq_client = zmq_client_manager

        self._topic1 = ConstStrings.EXAMPLE_TOPIC
        self._topic2 = ConstStrings.ANOTHER_TOPIC

        self._logger = LoggerFactory.get_logger_manager()

        self._init_producers()
        self._init_zmq_client()

    def _format_tagged(self, tag: str, color: str, msg: str) -> str:
        return f"{color}{tag}{ConstColors.RESET} {msg}"

    def _log_zmq(self, msg: str) -> None:
        self._logger.log(
            ConstStrings.LOG_NAME_DEBUG,
            self._format_tagged(self._TAG_ZMQ_CLIENT, self._TAG_ZMQ_CLIENT_COLOR, msg),
        )

    def _log_kafka(self, msg: str) -> None:
        self._logger.log(
            ConstStrings.LOG_NAME_DEBUG,
            self._format_tagged(self._TAG_KAFKA, self._TAG_KAFKA_COLOR, msg),
        )

    def do_something(self) -> None:
        self._kafka_manager.send_message(self._topic1, "Manual do_something message")

    def _init_producers(self) -> None:
        t1 = threading.Thread(target=self._produce_topic1_messages, daemon=True)
        t2 = threading.Thread(target=self._produce_topic2_messages, daemon=True)
        t1.start()
        t2.start()

    def _produce_topic1_messages(self) -> None:
        while True:
            time.sleep(Consts.SEND_MESSAGE_DURATION * 2)
            self._kafka_manager.send_message(self._topic1, ConstStrings.EXAMPLE_MESSAGE)

    def _produce_topic2_messages(self) -> None:
        while True:
            time.sleep(Consts.SEND_MESSAGE_DURATION * 2)
            self._kafka_manager.send_message(self._topic2, ConstStrings.ANOTHER_MESSAGE)

    def _init_zmq_client(self) -> None:
        try:
            self._zmq_client.start()
            self._log_zmq(LoggerMessages.ZMQ_CLIENT_STARTED if hasattr(LoggerMessages, "ZMQ_CLIENT_STARTED")
                          else "ZMQ client started")
        except Exception as e:
            self._log_zmq(
                LoggerMessages.ZMQ_CLIENT_THREAD_ERROR.format(e)
                if hasattr(LoggerMessages, "ZMQ_CLIENT_THREAD_ERROR")
                else f"ZMQ client start error: {e}"
            )

        thread = threading.Thread(target=self._zmq_request_loop, daemon=True)
        thread.start()

    def _zmq_request_loop(self) -> None:
        counter = 0
        while True:
            time.sleep(10)

            message_text = f"Hello from producer ZMQ client #{counter}"
            counter += 1

            if hasattr(LoggerMessages, "ZMQ_CLIENT_GENERATED_MESSAGE"):
                self._log_zmq(LoggerMessages.ZMQ_CLIENT_GENERATED_MESSAGE.format(message_text))
            else:
                self._log_zmq(f"Generated ZMQ message: {message_text}")

            req = Request(
                resource=ConstStrings.EXAMPLE_RESOURCE,
                operation=ConstStrings.EXAMPLE_OPERATION,
                data={
                    "topic": ConstStrings.EXAMPLE_TOPIC,
                    "message": message_text,
                },
            )

            if hasattr(LoggerMessages, "ZMQ_CLIENT_SENDING_REQUEST"):
                self._log_zmq(LoggerMessages.ZMQ_CLIENT_SENDING_REQUEST.format(req.to_json()))
            else:
                self._log_zmq(f"Sending ZMQ request: {req.to_json()}")


            res = self._zmq_client.send_request(req)

            if hasattr(LoggerMessages, "ZMQ_CLIENT_RECEIVED_RESPONSE"):
                self._log_zmq(LoggerMessages.ZMQ_CLIENT_RECEIVED_RESPONSE.format(res.to_json()))
            else:
                self._log_zmq(f"Received ZMQ response: {res.to_json()}")

