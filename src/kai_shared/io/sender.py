import zmq
import zmq.asyncio

from kai_shared.config_shared import EndpointConfig
from kai_shared.schemata.ipc import TelemetryPing
from kai_shared.utils.logger import get_logger

logger = get_logger(__name__)


class PublisherRealtime:
    def __init__(self, config_shared: EndpointConfig):
        self.address = config_shared.address_realtime
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDHWM, 1)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.bind(self.address)
        logger.info(f"RealtimePublisher bound to {self.address}")

    async def send(self, payload: bytes) -> None:
        try:
            await self.socket.send(payload, copy=False)
        except zmq.ZMQError as e:
            logger.error(f"Error publishing realtime payload: {e}")

    def close(self) -> None:
        self.socket.close()


class SenderSequential:
    def __init__(self, config_shared: EndpointConfig):
        self.address = config_shared.address_sequential
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.PUSH)
        self.socket.setsockopt(zmq.SNDHWM, 1000)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.bind(self.address)
        logger.info(f"ReliableSender bound to {self.address}")

    async def send(self, payload: bytes) -> None:
        try:
            await self.socket.send(payload, copy=False)
        except zmq.ZMQError as e:
            logger.error(f"Error sending reliable payload: {e}")

    def close(self) -> None:
        self.socket.close()


class DealerTelemetry:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt_string(zmq.IDENTITY, self.node_id)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.setsockopt(zmq.SNDHWM, 1)
        self.socket.setsockopt(zmq.RCVHWM, 10)

    def connect(self, peer_config_shared: EndpointConfig) -> None:
        self.socket.connect(peer_config_shared.address_system)
        logger.info(f"TelemetryDealer connected to {peer_config_shared.address_system}")

    async def send_ping(self, timestamp: float) -> None:
        try:
            ping = TelemetryPing(
                origin_id=self.node_id, responder_id="", timestamp=timestamp
            )
            ping_bytes = ping.model_dump_json().encode("utf-8")
            await self.socket.send(ping_bytes, flags=zmq.NOBLOCK)
        except zmq.Again:
            logger.warning("Ping dropped: destination unreachable or socket queue full")
        except zmq.ZMQError as e:
            logger.error(f"Error sending ping: {e}")

    def close(self) -> None:
        self.socket.close()
