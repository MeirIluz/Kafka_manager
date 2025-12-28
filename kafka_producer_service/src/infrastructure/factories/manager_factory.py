import os

from infrastructure.factories.infrastructure_factory import InfrastructureFactory
from infrastructure.factories.api_factory import ApiFactory

from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from infrastructure.interfaces.izmq_server_manager import IZmqServerManager
from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.izmq_client_manager import IZmqClientManager
from infrastructure.events.zmq_client_manager import ZmqClientManager

from globals.consts.const_strings import ConstStrings
from model.managers.example_manager import ExampleManager


class ManagerFactory:
    @staticmethod
    def create_config_manager(config_path: str) -> IConfigManager:
        return InfrastructureFactory.create_config_manager(config_path)

    @staticmethod
    def create_kafka_manager(config_manager: IConfigManager) -> IKafkaManager:
        return InfrastructureFactory.create_kafka_manager(config_manager)

    @staticmethod
    def create_zmq_client_manager() -> IZmqClientManager:
        host = os.getenv(ConstStrings.ZMQ_SERVER_HOST)
        port = os.getenv(ConstStrings.ZMQ_SERVER_PORT)
        return ZmqClientManager(host, port)
    
    @staticmethod
    def create_example_manager(
        config_manager: IConfigManager,
        kafka_manager: IKafkaManager,
        zmq_client_manager: IZmqClientManager,
    ) -> IExampleManager:
        return ExampleManager(config_manager, kafka_manager, zmq_client_manager)

    @staticmethod
    def create_all() -> IExampleManager:
        config_path = os.getenv("CONFIG_PATH", ConstStrings.GLOBAL_CONFIG_PATH)

        config_manager = ManagerFactory.create_config_manager(config_path)
        kafka_manager = ManagerFactory.create_kafka_manager(config_manager)

        zmq_client_manager = ManagerFactory.create_zmq_client_manager()
        zmq_client_manager.start()

        return ManagerFactory.create_example_manager(config_manager, kafka_manager, zmq_client_manager)
