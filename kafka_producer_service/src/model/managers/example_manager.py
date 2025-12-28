import time
import threading

from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from infrastructure.interfaces.izmq_client_manager import IZmqClientManager
from globals.consts.const_strings import ConstStrings
from globals.consts.logger_messages import LoggerMessages
from globals.consts.const_colors import ConstColors
from infrastructure.factories.logger_factory import LoggerFactory


class ExampleManager(IExampleManager):
    _TAG_ZMQ_CLIENT = LoggerMessages.TAG_ZMQ_CLIENT
    _TAG_ZMQ_CLIENT_COLOR = ConstColors.CYAN

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

        self._logger = LoggerFactory.get_logger_manager()

        # start zmq client
        self._zmq_client.start()
        self._logger.log(
            ConstStrings.LOG_NAME_DEBUG,
            f"{ConstColors.CYAN}{LoggerMessages.TAG_ZMQ_CLIENT}{ConstColors.RESET} ZMQ client started",
        )

        # run demo loop
        t = threading.Thread(target=self._demo_loop, daemon=True)
        t.start()

        # keep process alive
        while True:
            time.sleep(60)

    def do_something(self) -> None:
        return

    def _demo_loop(self) -> None:
        counter = 0
        while True:
            time.sleep(10)

            msg = f"Hello from internal ZMQ client #{counter}"
            counter += 1

            self._logger.log(
                ConstStrings.LOG_NAME_DEBUG,
                f"{ConstColors.CYAN}{LoggerMessages.TAG_ZMQ_CLIENT}{ConstColors.RESET} "
                f"{LoggerMessages.ZMQ_CLIENT_GENERATED_MESSAGE.format(msg)}",
            )

            # send request via your ZmqClientManager abstraction
            request = {
                ConstStrings.RESOURCE_IDENTIFIER: ConstStrings.EXAMPLE_RESOURCE,
                ConstStrings.OPERATION_IDENTIFIER: ConstStrings.EXAMPLE_OPERATION,
                ConstStrings.DATA_IDENTIFIER: {
                    "topic": ConstStrings.EXAMPLE_TOPIC,
                    "message": msg,
                },
            }

            self._logger.log(
                ConstStrings.LOG_NAME_DEBUG,
                f"{ConstColors.CYAN}{LoggerMessages.TAG_ZMQ_CLIENT}{ConstColors.RESET} "
                f"{LoggerMessages.ZMQ_CLIENT_SENDING_REQUEST.format(request)}",
            )

            response = self._zmq_client.send_request_from_dict(request)  # see note below

            self._logger.log(
                ConstStrings.LOG_NAME_DEBUG,
                f"{ConstColors.CYAN}{LoggerMessages.TAG_ZMQ_CLIENT}{ConstColors.RESET} "
                f"{LoggerMessages.ZMQ_CLIENT_RECEIVED_RESPONSE.format(response)}",
            )
