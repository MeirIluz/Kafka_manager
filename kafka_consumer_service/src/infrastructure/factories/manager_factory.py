import os
from infrastructure.factories.infrastructure_factory import InfrastructureFactory
from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.izmq_server_manager import IZmqServerManager
from model.managers.example_manager import ExampleManager
from infrastructure.factories.api_factory import ApiFactory


class ManagerFactory:
    _kafka_manager = None
    _zmq_server_manager = None

    def _get_config_path() -> str:
        factories_dir = os.path.dirname(os.path.abspath(__file__))
        infra_root = os.path.dirname(factories_dir)
        config_path = os.path.join(infra_root, "config", "configuration.xml")
        return config_path

    @staticmethod
    def create_example_manager() -> IExampleManager:
        config_path = ManagerFactory._get_config_path()
        config_manager = InfrastructureFactory.create_config_manager(
            config_path)

        kafka_manager = InfrastructureFactory.create_kafka_manager(
            config_manager)
        ManagerFactory._kafka_manager = kafka_manager

        return ExampleManager(config_manager, kafka_manager)

    @staticmethod
    def create_example_zmq_manager() -> IZmqServerManager:
        kafka_manager = ManagerFactory._kafka_manager
        routers = ApiFactory.create_routers(kafka_manager)
        zmq_server_manager = InfrastructureFactory.create_zmq_server_manager(
            routers)


        zmq_server_manager.start()

        ManagerFactory._zmq_server_manager = zmq_server_manager
        return zmq_server_manager

    @staticmethod
    def create_all():
        mode = os.getenv("APP_MODE", "all").strip().lower()

        # This creates KafkaManager + ExampleManager (ExampleManager decides what to start)
        ManagerFactory.create_example_manager()

        # Only producer/all should run the ZMQ server
        if mode in ("producer", "all"):
            ManagerFactory.create_example_zmq_manager()
