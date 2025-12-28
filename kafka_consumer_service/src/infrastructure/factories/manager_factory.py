import os

from infrastructure.factories.infrastructure_factory import InfrastructureFactory
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from infrastructure.interfaces.iexample_manager import IExampleManager
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
    def create_example_manager(
        config_manager: IConfigManager,
        kafka_manager: IKafkaManager,
    ) -> IExampleManager:
        return ExampleManager(config_manager, kafka_manager)

    @staticmethod
    def create_all() -> IExampleManager:

        config_manager = ManagerFactory.create_config_manager(ConstStrings.GLOBAL_CONFIG_PATH)
        kafka_manager = ManagerFactory.create_kafka_manager(config_manager)
        return ManagerFactory.create_example_manager(config_manager, kafka_manager)
