from typing import Literal

from pydantic import BaseModel


class TelemetryPing(BaseModel):
    origin_id: str
    responder_id: str
    timestamp: float


class TokenStreamMetadata(BaseModel):
    stream_type: Literal["token"] = "token"
    request_id: str
    is_final: bool


class AudioStreamMetadata(BaseModel):
    stream_type: Literal["audio"] = "audio"
    request_id: str
    is_final: bool
    sample_rate: int
    dtype: str
    channels: int = 1
