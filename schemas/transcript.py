from pydantic import BaseModel

class TranscriptSchema(BaseModel):
    audio_id: str
    interval: str

