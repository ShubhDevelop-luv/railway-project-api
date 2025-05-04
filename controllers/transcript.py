from fastapi import APIRouter, HTTPException, Depends
from db.postgres_management import PostgresManagement
from models.transcript import Transcript
from schemas.transcript import TranscriptSchema
from utils.celery_worker import process_transcription_job
from controllers.auth_middleware import *

router = APIRouter()
db = PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )

@router.post("/create_transcription_job")
def create_transcription_job(data: TranscriptSchema = Depends(), current_user: str = Depends(get_current_user)):
    """
    User selects an interval after uploading an audio file.
    This triggers background transcript processing.
    """
    if data.interval not in ["1min", "5min"]:
        raise HTTPException(status_code=400, detail="Invalid interval. Choose '1min' or '5min'.")

    try:
        transcript_record = Transcript(
            id=None,
            audio_file_id=data.audio_id,
            interval=data.interval,
            job_status="pending",
            transcript_blob_url=None,
            transcript_filename=None,
            created_at="NOW()",
            updated_at="NOW()"
        )

        job_id = db.insert_record("transcription_jobs", transcript_record.__dict__)["id"]
        audio_blob_url = db.find_record("audio_files", "id=%s", (data.audio_id,))["blob_url"]
        
        process_transcription_job.delay(job_id, audio_blob_url, data.interval)

        return {"job_id": job_id, "message": f"Transcription job started for {data.interval} interval."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create transcript job: {str(e)}")


@router.get("/get_transcript/{job_id}")
def get_transcript(job_id: int, current_user: str = Depends(get_current_user)):
    """
    Retrieve the completed transcript for a given job.
    """
    try:
        transcript = db.find_record("transcription_jobs", "id=%s AND job_status=%s", (job_id, "completed"))

        if not transcript:
            return {"status": False, "message": "Transcript processing in progress. Please wait."}

        return {
            "status": True,
            "message": "Transcript available",
            "transcript_blob_url": transcript["transcript_blob_url"],
            "transcript_filename": transcript["transcript_filename"]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve transcript: {str(e)}")
    

# @router.get("/transcription_history/{user_id}")
# def get_transcription_history(user_id: int, date: str):
#     """
#     Retrieve transcription history for a user by date.
#     """
#     try:
#         transcripts = db.find_all_records("transcription_jobs", 
#             "created_at::DATE = %s AND audio_file_id IN (SELECT id FROM audio_files WHERE user_id=%s)", 
#             (date, user_id))

#         return {"status": True, "transcripts": transcripts}
    
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to retrieve history: {str(e)}")