from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from db.postgres_management import PostgresManagement
from models.audio import Audio
from schemas.audio import AudioUploadSchema
import uuid
from controllers.auth_middleware import *
from utils.azure_blob import BlobStorage

router = APIRouter()
db = PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )
blob = BlobStorage()

AUDIO_CONTAINER = "audiofiles"
TRANSCRIPT_CONTAINER = "transcripts"

@router.post("/upload_audio")
async def upload_audio(file: UploadFile = File(...), data: AudioUploadSchema = Depends(), current_user: str = Depends(get_current_user)):
    """
    Upload an audio file and store metadata in PostgreSQL.
    """
    try:
        blob_url = blob.upload_file(file.filename, file.file, container_name=AUDIO_CONTAINER, content_type=file.content_type)
        audio_record = Audio(
            id=None,
            user_id=data.user_id,
            filename=file.filename,
            blob_url=blob_url,
            status="uploaded",
            uploaded_at="NOW()"
        )

        audio_id = db.insert_record("audio_files", audio_record.__dict__)["id"]

        return {"status": True, "message": "Audio uploaded successfully", "audio_id": audio_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")