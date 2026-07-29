import zmq
import zmq.asyncio
import asyncio
from typing import Callable, Awaitable, Optional
from kai_shared.utils.logger import get_logger
from kai_shared.config_shared import EndpointConfig

logger = get_logger(__name__)


class DataSubscriber:
    def __init__(self):
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.RCVHWM, 100)
        self._callback: Optional[Callable[[bytes, bytes, bytes], Awaitable[None]]] = (
            None
        )

    def connect(self, peer_config_shared: EndpointConfig) -> None:
        self.socket.connect(peer_config_shared.data_address)
        logger.info(f"DataSubscriber connected to {peer_config_shared.data_address}")

    def subscribe(self, topic: bytes) -> None:
        self.socket.setsockopt(zmq.SUBSCRIBE, topic)

    def register_callback(
        self, callback: Callable[[bytes, bytes, bytes], Awaitable[None]]
    ) -> None:
        self._callback = callback

    async def listen(self) -> None:
        while True:
            try:
                multipart_data = await self.socket.recv_multipart()
                if len(multipart_data) == 3 and self._callback:
                    topic, metadata_bytes, payload = multipart_data
                    await self._callback(topic, metadata_bytes, payload)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in subscriber loop: {e}")

    def close(self) -> None:
        self.socket.close()


class TelemetryRouter:
    def __init__(self, config_shared: EndpointConfig):
        self.address = config_shared.control_address
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.SNDHWM, 10)
        self.socket.setsockopt(zmq.RCVHWM, 10)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.bind(self.address)
        self._callback: Optional[Callable[[bytes, bytes], Awaitable[None]]] = None
        logger.info(f"TelemetryRouter bound to {self.address}")

    def register_callback(
        self, callback: Callable[[bytes, bytes], Awaitable[None]]
    ) -> None:
        self._callback = callback

    async def listen(self) -> None:
        while True:
            try:
                identity, message = await self.socket.recv_multipart()
                if self._callback:
                    await self._callback(identity, message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in telemetry router loop: {e}")

    async def send_pong(self, identity: bytes, message: bytes) -> None:
        try:
            await self.socket.send_multipart([identity, message], flags=zmq.NOBLOCK)
        except zmq.Again:
            logger.warning(
                f"Pong dropped for {identity.hex()}: socket queue full or unreachable"
            )
        except Exception as e:
            logger.error(f"Error sending pong: {e}")

    def close(self) -> None:
        self.socket.close()
