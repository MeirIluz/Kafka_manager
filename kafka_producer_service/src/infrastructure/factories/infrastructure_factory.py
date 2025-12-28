from infrastructure.interfaces.ikafka_manager import IKafkaManager
from infrastructure.events.kafka_manager import KafkaManager

from infrastructure.interfaces.ievent_manager import IEventManager
from infrastructure.events.event_manager import EventManager

from infrastructure.config.xml_config_manager import XMLConfigManager
from infrastructure.interfaces.iconfig_manager import IConfigManager

from infrastructure.interfaces.izmq_client_manager import IZmqClientManager
from infrastructure.events.zmq_client_manager import ZmqClientManager


class InfrastructureFactory:
    _event_manager: IEventManager = None

    @staticmethod
    def create_config_manager(config_path: str) -> IConfigManager:
        return XMLConfigManager(config_path)

    @staticmethod
    def create_kafka_manager(config_manager: IConfigManager) -> IKafkaManager:
        return KafkaManager(config_manager)

    @staticmethod
    def create_event_manager() -> IEventManager:
        if InfrastructureFactory._event_manager is None:
            InfrastructureFactory._event_manager = EventManager()
        return InfrastructureFactory._event_manager

    @staticmethod
    def create_zmq_client_manager(host: str, port: str) -> IZmqClientManager:
        return ZmqClientManager(host, int(port))
