import json
import os
import threading
from typing import Callable, Iterable, Optional

from kafka import KafkaProducer, KafkaConsumer

from globals.consts.const_strings import ConstStrings
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.factories.logger_factory import LoggerFactory
from globals.consts.logger_messages import LoggerMessages


class KafkaManager(IKafkaManager):
    def __init__(self, config_manager: IConfigManager) -> None:
        self._producer = None
        self._consumer: Optional[KafkaConsumer] = None
        self._consumer_thread: Optional[threading.Thread] = None

        self._bootstrap_servers = None
        self._config_manager = config_manager
        self._logger = LoggerFactory.get_logger_manager()

        self._init_data_from_configuration()
        self._init_kafka_producer()

    def send_message(self, topic: str, msg: str) -> None:
        if self._config_manager.exists(topic):
            self._producer.send(topic, value=msg)
            self._producer.flush()

    def start_consuming(self, topics: Iterable[str], callback: Callable) -> None:
        # Start ONE consumer subscribed to multiple topics.
        # This prevents "2 members" issue and duplicate-looking logs.
        if self._consumer_thread is not None:
            self._logger.log(
                ConstStrings.LOG_NAME_DEBUG,
                LoggerMessages.KAFKA_TOPIC_ALREADY_CONSUMING.format(list(topics)),
            )
            return

        valid_topics = [t for t in topics if self._config_manager.exists(t)]
        if not valid_topics:
            self._logger.log(ConstStrings.LOG_NAME_DEBUG, LoggerMessages.KAFKA_TOPIC_NOT_EXIST)
            return

        self._consumer = KafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset=ConstStrings.AUTO_OFFSET_RESET,
            enable_auto_commit=False,  # manual commit
            group_id=os.getenv("KAFKA_GROUP_ID", ConstStrings.GROUP_ID),
            value_deserializer=lambda m: json.loads(m.decode(ConstStrings.DECODE_FORMAT)),
        )
        self._consumer.subscribe(valid_topics)

        self._consumer_thread = threading.Thread(
            target=self._consume_loop, args=(self._consumer, callback), daemon=False
        )
        self._consumer_thread.start()

    def _init_data_from_configuration(self) -> None:
        self._bootstrap_servers = self._config_manager.get(
            ConstStrings.KAFKA_ROOT_CONFIGURATION_NAME,
            ConstStrings.BOOTSTRAP_SERVERS_ROOT,
        )

    def _init_kafka_producer(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(ConstStrings.ENCODE_FORMAT),
        )

    def _consume_loop(self, consumer: KafkaConsumer, callback: Callable) -> None:
        for message in consumer:
            callback(message.topic, message.value)
            consumer.commit()