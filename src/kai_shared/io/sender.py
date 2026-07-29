import zmq
import zmq.asyncio
from kai_shared.utils.logger import get_logger
from kai_shared.config_shared import EndpointConfig
from kai_shared.schemata.ipc import StreamMetadata, TelemetryPing

logger = get_logger(__name__)


class DataPublisher:
    def __init__(self, config_shared: EndpointConfig):
        self.address = config_shared.data_address
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.setsockopt(zmq.SNDHWM, 100)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.bind(self.address)
        logger.info(f"DataPublisher bound to {self.address}")

    async def send_stream(
        self, topic: bytes, metadata: StreamMetadata, payload: bytes
    ) -> None:
        try:
            metadata_bytes = metadata.model_dump_json().encode("utf-8")
            await self.socket.send_multipart(
                [topic, metadata_bytes, payload], copy=False
            )
        except Exception as e:
            logger.error(f"Error publishing stream data: {e}")

    def close(self) -> None:
        self.socket.close()


class TelemetryDealer:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.setsockopt_string(zmq.IDENTITY, self.node_id)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.setsockopt(zmq.SNDHWM, 1)
        self.socket.setsockopt(zmq.RCVHWM, 10)

    def connect(self, peer_config_shared: EndpointConfig) -> None:
        self.socket.connect(peer_config_shared.control_address)
        logger.info(
            f"TelemetryDealer connected to {peer_config_shared.control_address}"
        )

    async def send_ping(self, timestamp: float) -> None:
        try:
            ping = TelemetryPing(
                origin_id=self.node_id, responder_id="", timestamp=timestamp
            )
            ping_bytes = ping.model_dump_json().encode("utf-8")
            await self.socket.send(ping_bytes, flags=zmq.NOBLOCK)
        except zmq.Again:
            logger.warning("Ping dropped: destination unreachable or socket queue full")
        except Exception as e:
            logger.error(f"Error sending ping: {e}")

    def close(self) -> None:
        self.socket.close()
