import asyncio
import time
import zmq
from pydantic import ValidationError
from kai_shared.config_shared import SharedConfig
from kai_shared.io.receiver import (
    SubscriberRealtime,
    ReceiverSequential,
    RouterTelemetry,
)
from kai_shared.io.sender import PublisherRealtime, SenderSequential, DealerTelemetry
from kai_shared.schemata.ipc import TelemetryPing
from kai_shared.utils.logger import get_logger

logger = get_logger(__name__)


class PipelineNode:
    def __init__(self, config: SharedConfig):
        self.config = config
        self.node_id = self.config.network.node_id

        self.publisher_realtime = PublisherRealtime(self.config.network.bind)
        self.sender_sequential = SenderSequential(self.config.network.bind)
        self.router = RouterTelemetry(self.config.network.bind)

        self.realtime_subscriber = SubscriberRealtime()
        self.reliable_receiver = ReceiverSequential()
        self.dealer = DealerTelemetry(self.node_id)

        for peer in self.config.network.peers:
            self.realtime_subscriber.connect(peer)
            self.reliable_receiver.connect(peer)
            self.dealer.connect(peer)

        self.realtime_subscriber.register_callback(self.handle_realtime)
        self.reliable_receiver.register_callback(self.handle_reliable)
        self.router.register_callback(self.handle_ping)

        self._running = False
        self._tasks: list[asyncio.Task] = []

    async def handle_realtime(self, payload: bytes) -> None:
        logger.debug(f"Received realtime payload of size {len(payload)}")

    async def handle_reliable(self, payload: bytes) -> None:
        logger.debug(f"Received reliable payload of size {len(payload)}")

    async def send_realtime(self, payload: bytes) -> None:
        await self.publisher_realtime.send(payload)

    async def send_reliable(self, payload: bytes) -> None:
        await self.sender_sequential.send(payload)

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
        max_rtt_ms = float(self.config.network.ping_interval * 1000)
        while self._running:
            try:
                message = await self.dealer.socket.recv()
                ping = TelemetryPing.model_validate_json(message)
                rtt = (time.monotonic() - ping.timestamp) * 1000
                if rtt > max_rtt_ms:
                    logger.warning(
                        f"[TELEMETRY] Dropped stale ping response from {ping.responder_id} (RTT: {rtt:.2f} ms)"
                    )
                else:
                    logger.info(f"[TELEMETRY] RTT to {ping.responder_id}: {rtt:.2f} ms")
            except ValidationError as e:
                logger.error(f"Malformed telemetry response: {e}")
            except asyncio.CancelledError:
                break
            except zmq.ZMQError:
                logger.error("Error receiving telemetry response")

    async def start(self) -> None:
        self._running = True
        self._tasks.append(asyncio.create_task(self.realtime_subscriber.listen()))
        self._tasks.append(asyncio.create_task(self.reliable_receiver.listen()))
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
        self.publisher_realtime.close()
        self.sender_sequential.close()
        self.realtime_subscriber.close()
        self.reliable_receiver.close()
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
