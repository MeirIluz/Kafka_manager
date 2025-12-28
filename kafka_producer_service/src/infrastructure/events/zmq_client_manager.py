import zmq

from globals.enums.response_status import ResponseStatus
from globals.consts.const_strings import ConstStrings
from infrastructure.interfaces.izmq_client_manager import IZmqClientManager
from globals.consts.logger_messages import LoggerMessages
from model.data_classes.zmq_request import Request
from model.data_classes.zmq_response import Response


class ZmqClientManager(IZmqClientManager):
    def __init__(self, host: str, port: int):
        self._context = zmq.Context.instance()

        self._host = host
        self._port = port
        self._address = f"{ConstStrings.BASE_TCP_CONNECTION_STRINGS}{host}:{port}"

        self._socket = None
        self._connect()

    def _connect(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close(linger=0)
            except Exception:
                pass

        self._socket = self._context.socket(zmq.REQ)

        # Prevent hanging forever
        self._socket.RCVTIMEO = 3000  # ms
        self._socket.SNDTIMEO = 3000  # ms
        self._socket.LINGER = 0

        self._socket.connect(self._address)

    def start(self) -> None:
        # Keeping it for interface compatibility
        return

    def stop(self) -> None:
        try:
            if self._socket is not None:
                self._socket.close(linger=0)
        finally:
            self._socket = None

    def send_request(self, request: Request) -> Response:
        try:
            self._socket.send_json(request.to_json())
            response = self._socket.recv_json()
            return Response.from_json(response)

        except zmq.Again:
            self._logger.log(
                ConstStrings.LOG_NAME_DEBUG,
                LoggerMessages.ZMQ_CLIENT_RECV_TIMEOUT,
            )
            return Response(
                status=ResponseStatus.TIMEOUT,
                data={}
            )

        except Exception as e:
            self._logger.log(
                ConstStrings.LOG_NAME_DEBUG,
                LoggerMessages.ZMQ_CLIENT_THREAD_ERROR.format(str(e)),
            )
            return Response(
                status=ResponseStatus.ERROR,
                data={ConstStrings.ERROR_MESSAGE: str(e)}
            )

