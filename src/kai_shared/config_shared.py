from ipaddress import IPv4Address

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from kai_shared.utils.custom_types import LogLevel, NetworkProtocol


class EndpointConfig(BaseModel):
    protocol: NetworkProtocol
    host: str | IPv4Address
    port_realtime: int
    port_sequential: int
    port_system: int

    @property
    def address_realtime(self) -> str:
        return f"{self.protocol.value}{self.host}:{self.port_realtime}"

    @property
    def address_sequential(self) -> str:
        return f"{self.protocol.value}{self.host}:{self.port_sequential}"

    @property
    def address_system(self) -> str:
        return f"{self.protocol.value}{self.host}:{self.port_system}"


class SystemConfig(BaseModel):
    log_level: LogLevel = LogLevel.INFO


class NetworkConfig(BaseModel):
    node_id: str = "unnamed"
    ping_interval: float = 1.0
    enable_telemetry: bool = False
    bind: EndpointConfig = EndpointConfig(
        protocol=NetworkProtocol.TCP,
        host="0.0.0.0",
        port_realtime=1997,
        port_sequential=1998,
        port_system=1999,
    )
    peers: list[EndpointConfig] = []


class SharedConfig(BaseSettings):
    system: SystemConfig = SystemConfig()
    network: NetworkConfig = NetworkConfig()


settings_shared = SharedConfig()
