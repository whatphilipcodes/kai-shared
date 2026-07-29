from pydantic import BaseModel


class TelemetryPing(BaseModel):
    origin_id: str
    responder_id: str
    timestamp: float


class StreamMetadata(BaseModel):
    request_id: str
    is_final: bool
    stream_type: str
