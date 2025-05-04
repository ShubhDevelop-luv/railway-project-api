from pydantic import BaseModel

class HistorySchema(BaseModel):
    user_id: str
    date: str  # YYYY-MM-DD format


