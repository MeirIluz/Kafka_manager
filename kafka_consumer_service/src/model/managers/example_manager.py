import os
import json
import threading
from typing import Callable, Optional

import zmq

from kafka import KafkaConsumer
from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from globals.consts.const_strings import ConstStrings
from globals.consts.consts import Consts
from globals.consts.logger_messages import LoggerMessages
from globals.consts.const_colors import ConstColors
from infrastructure.factories.logger_factory import LoggerFactory
from test_modules.debug_print import DEBUG_PRINT


class ExampleManager(IExampleManager):
    def __init__(self, config_manager: IConfigManager, topic: str) -> None:
        self._topic = topic
        self._bootstrap_servers: Optional[str] = None
        self._consumer: Optional[KafkaConsumer] = None
        self._config_manager = config_manager

        self._thread: Optional[threading.Thread] = None
        self._is_consuming = False

        self._init_data_from_configuration()
        self._init_kafka_consumer()

    def start_consuming(self, callback: Callable) -> None:
        if self._consumer is None:
            DEBUG_PRINT("Consumer not initialized (topic invalid or config missing).")
            return

        if self._is_consuming:
            DEBUG_PRINT(f"Already consuming topic: {self._topic}")
            return

        self._is_consuming = True
        self._thread = threading.Thread(target=self._consume, args=(callback,), daemon=True)
        self._thread.start()

    def _init_data_from_configuration(self) -> None:
        if not self._config_manager.exists(self._topic):
            DEBUG_PRINT(f"TOPIC NOT EXIST: {self._topic}")
            self._topic = None
            return

        self._bootstrap_servers = self._config_manager.get(
            ConstStrings.KAFKA_ROOT_CONFIGURATION_NAME,
            ConstStrings.BOOTSTRAP_SERVERS_ROOT,
        )

    def _init_kafka_consumer(self) -> None:
        group_id = os.getenv("KAFKA_GROUP_ID", ConstStrings.GROUP_ID)

        self._consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset=ConstStrings.AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode(ConstStrings.ENCODE_FORMAT)),
        )

    def _consume(self, callback:Callable)->None:
        for message in self._consumer:
            callback(message.value)