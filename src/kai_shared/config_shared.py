from typing import Tuple, Type
from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)
from ipaddress import IPv4Address
from src.kai_shared.utils.custom_types import LogLevel, NetworkProtocol


class EndpointConfig(BaseModel):
    protocol: NetworkProtocol
    host: str | IPv4Address
    data_port: int
    control_port: int

    @property
    def data_address(self) -> str:
        return f"{self.protocol.value}{self.host}:{self.data_port}"

    @property
    def control_address(self) -> str:
        return f"{self.protocol.value}{self.host}:{self.control_port}"


class SystemConfig(BaseModel):
    log_level: LogLevel = LogLevel.INFO


class NetworkConfig(BaseModel):
    node_id: str = "node-unnamed"
    ping_interval: float = 1.0
    bind: EndpointConfig = EndpointConfig(
        protocol=NetworkProtocol.TCP, host="0.0.0.0", data_port=8080, control_port=8080
    )
    peers: list[EndpointConfig] = []


class SharedConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file="config.toml")
    system: SystemConfig = SystemConfig()
    network: NetworkConfig = NetworkConfig()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (TomlConfigSettingsSource(settings_cls),)


settings = SharedConfig()
