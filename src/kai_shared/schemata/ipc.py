from pydantic import BaseModel


class StreamMetadata(BaseModel):
    origin_id: str
    monotonic_timestamp: float
    stream_type: str


class VideoMetadata(StreamMetadata):
    frame_index: int
    width: int
    height: int
    codec: str
    is_keyframe: bool


class AudioMetadata(StreamMetadata):
    sample_rate: int
    channels: int
    chunk_index: int


class TokenMetadata(StreamMetadata):
    sequence_index: int
    is_final: bool


class TelemetryPing(BaseModel):
    origin_id: str
    responder_id: str
    timestamp: float
