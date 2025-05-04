from fastapi import APIRouter, HTTPException, Depends
from db.postgres_management import PostgresManagement
from models.history import History
from schemas.history import HistorySchema
from controllers.auth_middleware import *

router = APIRouter()
db = PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )

@router.get("/transcription_history")
def get_transcription_history(data: HistorySchema = Depends(), current_user: str = Depends(get_current_user)):
    """
    Retrieve transcription history for a user by date.
    """
    try:
        transcripts = db.find_all_records("transcription_jobs", 
            "created_at::DATE = %s AND audio_file_id IN (SELECT id FROM audio_files WHERE user_id=%s)", 
            (data.date, data.user_id))

        history_record = History(
            user_id=data.user_id,
            date=data.date,
            transcripts=transcripts
        )

        return {"status": True, "history": history_record.__dict__}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve history: {str(e)}")