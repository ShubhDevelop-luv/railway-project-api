from pydantic import BaseModel


class AudioUploadSchema(BaseModel):
    user_id: int
    filename: str
    content_type: str