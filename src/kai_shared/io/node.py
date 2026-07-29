import time
import asyncio
from pydantic import ValidationError
from src.kai_shared.utils.logger import get_logger
from src.kai_shared.config_shared import SharedConfig
from src.kai_shared.schemata.ipc import TelemetryPing
from src.kai_shared.io.sender import DataPublisher, TelemetryDealer
from src.kai_shared.io.receiver import DataSubscriber, TelemetryRouter

logger = get_logger(__name__)


class PipelineNode:
    def __init__(self, config: SharedConfig):
        self.config = config
        self.node_id = self.config.network.node_id

        self.publisher = DataPublisher(self.config.network.bind)
        self.router = TelemetryRouter(self.config.network.bind)

        self.subscriber = DataSubscriber()
        self.dealer = TelemetryDealer(self.node_id)

        for peer in self.config.network.peers:
            self.subscriber.connect(peer)
            self.dealer.connect(peer)

        self.subscriber.subscribe(b"")

        self.subscriber.register_callback(self.handle_data)
        self.router.register_callback(self.handle_ping)

        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def handle_data(
        self, topic: bytes, metadata_bytes: bytes, payload: bytes
    ) -> None:
        logger.debug(f"Received data on topic {topic}")

    async def handle_ping(self, identity: bytes, message: bytes) -> None:
        try:
            ping_data = TelemetryPing.model_validate_json(message)
            ping_data.responder_id = self.node_id
            response_bytes = ping_data.model_dump_json().encode("utf-8")
            await self.router.send_pong(identity, response_bytes)
        except ValidationError as e:
            logger.error(f"Malformed ping received: {e}")

    async def _telemetry_loop(self) -> None:
        while self._running:
            await self.dealer.send_ping(time.monotonic())
            await asyncio.sleep(self.config.network.ping_interval)

    async def _telemetry_response_loop(self) -> None:
        while self._running:
            try:
                message = await self.dealer.socket.recv()
                ping = TelemetryPing.model_validate_json(message)
                rtt = (time.monotonic() - ping.timestamp) * 1000
                logger.info(f"[TELEMETRY] RTT to {ping.responder_id}: {rtt:.2f} ms")
            except ValidationError as e:
                logger.error(f"Malformed telemetry response: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error receiving telemetry response: {e}")

    async def start(self) -> None:
        self._running = True
        self._tasks.append(asyncio.create_task(self.subscriber.listen()))
        self._tasks.append(asyncio.create_task(self.router.listen()))
        self._tasks.append(asyncio.create_task(self._telemetry_loop()))
        self._tasks.append(asyncio.create_task(self._telemetry_response_loop()))
        logger.info(
            f"Node {self.node_id} started. Log level: {self.config.system.log_level}"
        )

    def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        self.publisher.close()
        self.subscriber.close()
        self.router.close()
        self.dealer.close()
        logger.info("Node resources released safely.")

    async def run(self) -> None:
        await self.start()
        print(f"\n--- Pipeline Node: {self.node_id} Running ---")
        try:
            while True:
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            print("\nPipeline node execution interrupted manually.")
        except asyncio.CancelledError:
            pass
        finally:
            self.stop()
