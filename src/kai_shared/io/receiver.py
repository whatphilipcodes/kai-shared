import asyncio
from collections.abc import Awaitable, Callable
import zmq
import zmq.asyncio
from kai_shared.config_shared import EndpointConfig
from kai_shared.utils.logger import get_logger

logger = get_logger(__name__)


class SubscriberRealtime:
    def __init__(self):
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.CONFLATE, 1)
        self.socket.setsockopt(zmq.RCVHWM, 1)
        self._callback: Callable[[bytes], Awaitable[None]] | None = None

    def connect(self, peer_config_shared: EndpointConfig) -> None:
        self.socket.connect(peer_config_shared.address_realtime)
        self.socket.setsockopt(zmq.SUBSCRIBE, b"")
        logger.info(
            f"RealtimeSubscriber connected to {peer_config_shared.address_realtime}"
        )

    def register_callback(self, callback: Callable[[bytes], Awaitable[None]]) -> None:
        self._callback = callback

    async def listen(self) -> None:
        while True:
            try:
                payload = await self.socket.recv()
                if self._callback:
                    await self._callback(payload)
            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                logger.error(f"Error in realtime subscriber loop: {e}")

    def close(self) -> None:
        self.socket.close()


class ReceiverSequential:
    def __init__(self):
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.setsockopt(zmq.RCVHWM, 1000)
        self._callback: Callable[[bytes], Awaitable[None]] | None = None

    def connect(self, peer_config_shared: EndpointConfig) -> None:
        self.socket.connect(peer_config_shared.address_sequential)
        logger.info(
            f"ReliableReceiver connected to {peer_config_shared.address_sequential}"
        )

    def register_callback(self, callback: Callable[[bytes], Awaitable[None]]) -> None:
        self._callback = callback

    async def listen(self) -> None:
        while True:
            try:
                payload = await self.socket.recv()
                if self._callback:
                    await self._callback(payload)
            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                logger.error(f"Error in reliable receiver loop: {e}")

    def close(self) -> None:
        self.socket.close()


class RouterTelemetry:
    def __init__(self, config_shared: EndpointConfig):
        self.address = config_shared.address_system
        self.context = zmq.asyncio.Context.instance()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.SNDHWM, 10)
        self.socket.setsockopt(zmq.RCVHWM, 10)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.bind(self.address)
        self._callback: Callable[[bytes, bytes], Awaitable[None]] | None = None
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
            except zmq.ZMQError as e:
                logger.error(f"Error in telemetry router loop: {e}")

    async def send_pong(self, identity: bytes, message: bytes) -> None:
        try:
            await self.socket.send_multipart([identity, message], flags=zmq.NOBLOCK)
        except zmq.Again:
            logger.warning(
                f"Pong dropped for {identity.hex()}: socket queue full or unreachable"
            )
        except zmq.ZMQError as e:
            logger.error(f"Error sending pong: {e}")

    def close(self) -> None:
        self.socket.close()
