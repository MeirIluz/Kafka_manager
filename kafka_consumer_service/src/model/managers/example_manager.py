from infrastructure.interfaces.iexample_manager import IExampleManager
from infrastructure.interfaces.iconfig_manager import IConfigManager
from infrastructure.interfaces.ikafka_manager import IKafkaManager
from globals.consts.const_strings import ConstStrings
from infrastructure.factories.logger_factory import LoggerFactory
from globals.consts.logger_messages import LoggerMessages
from globals.consts.const_colors import ConstColors


class ExampleManager(IExampleManager):
    def __init__(self, config_manager: IConfigManager, kafka_manager: IKafkaManager) -> None:
        super().__init__()
        self._config_manager = config_manager
        self._kafka_manager = kafka_manager
        self._logger = LoggerFactory.get_logger_manager()

        self._kafka_manager.start_consuming(
            [ConstStrings.EXAMPLE_TOPIC],
            self._on_message,
        )

    def do_something(self) -> None:
        return

    def _on_message(self, topic: str, msg):
        colored_msg = (
            f"{ConstColors.MAGENTA}{LoggerMessages.TAG_KAFKA}{ConstColors.RESET} "
            f"{ConstColors.CYAN}[{topic}]{ConstColors.RESET} , "
            f"message is: {ConstColors.GREEN}{msg}{ConstColors.RESET}"
        )

        self._logger.log(
            ConstStrings.LOG_NAME_DEBUG,
            colored_msg,
        )
