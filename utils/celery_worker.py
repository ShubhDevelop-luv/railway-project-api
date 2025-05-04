import os
import time
import csv
import asyncio
from fastapi import FastAPI, UploadFile, File, Query
import psycopg2
from celery import Celery
import redis
from utils.azure_blob import BlobStorage
from db.postgres_management import PostgresManagement


db = PostgresManagement(
        user="admin_railway",
        password="Welcome@3210",
        host="caasrailwat.postgres.database.azure.com",
        database="railwayproject",
        port=5432
    )
blob = BlobStorage()

# CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
# CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")

CELERY_BROKER_URL = redis.Redis(host=os.getenv("REDIS_HOST"),
        port=6380, db=0, password=os.getenv("REDIS_PWD"), ssl=True)
CELERY_RESULT_BACKEND = redis.Redis(host=os.getenv("REDIS_HOST"),
        port=6380, db=0, password=os.getenv("REDIS_PWD"), ssl=True)

celery_app = Celery("tasks", broker=CELERY_BROKER_URL, backend=CELERY_RESULT_BACKEND)


# -------------------------------
# CELERY TASK FOR TRANSCRIPTION
# -------------------------------
@celery_app.task
def process_transcription_job(job_id, audio_blob_url, interval, include_speaker=False):
    """
    Background task to process audio transcript asynchronously based on user-selected interval.
    Saves transcript in CSV format, with optional speaker diarization.
    """
    import os
    import logging
    import librosa
    import torch
    import whisperx
    import pandas as pd
    import csv
    from datetime import timedelta
    from utils.azure_blob import BlobStorage
    from db.postgres_management import PostgresManagement
    from utils.audio_processing import process_audio

    db = PostgresManagement()
    blob = BlobStorage()

    try:
        logging.info(f"Starting transcript generation for job_id: {job_id}, interval: {interval}, include_speaker: {include_speaker}")

        # Validate interval input
        if interval not in ["1min", "5min"]:
            raise ValueError("Invalid interval. Must be '1min' or '5min'.")

        # Step 1: Download audio file from Azure Blob Storage
        local_audio_path = f"/tmp/{job_id}_audio.wav"
        blob_data = blob.download_file(audio_blob_url, "audio-files")
        with open(local_audio_path, "wb") as f:
            f.write(blob_data)

        # Step 2: Preprocess the audio
        processed_audio_path = process_audio(local_audio_path)
        if not processed_audio_path:
            raise Exception("Audio processing failed.")

        # Step 3: Load WhisperX model
        DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
        COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "float32"
        model = whisperx.load_model("large", device=DEVICE, compute_type=COMPUTE_TYPE)

        # Step 4: Transcribe the audio
        transcription = model.transcribe(processed_audio_path, language="hi")
        aligned_result = whisperx.align(transcription["segments"], model, processed_audio_path, device=DEVICE)
        transcription["segments"] = aligned_result["segments"]

        # Step 5: Get audio duration using librosa
        y, sr = librosa.load(processed_audio_path, sr=None)
        audio_duration = librosa.get_duration(y=y, sr=sr)

        # Step 6: Run Speaker Diarization (Only if `include_speaker=True`)
        diarization_df = None
        if include_speaker:
            diarize_pipeline = whisperx.DiarizationPipeline(
                model_name="pyannote/speaker-diarization",
                use_auth_token="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",
                device=DEVICE
            )
            diarization_result = diarize_pipeline(processed_audio_path)
            diarization_df = pd.DataFrame(diarization_result) if not isinstance(diarization_result, pd.DataFrame) else diarization_result

        # Step 7: Generate interval-based transcript and save to CSV
        transcript_filename = f"{job_id}_transcript.csv"
        transcript_local_path = f"/tmp/{transcript_filename}"
        interval_seconds = 60 if interval == "1min" else 300  # 1min = 60 sec, 5min = 300 sec

        with open(transcript_local_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)

            if include_speaker:
                writer.writerow(["Interval Start", "Interval End", "Speaker", "Start Time", "End Time", "Text"])
            else:
                writer.writerow(["Interval Start", "Interval End", "Start Time", "End Time", "Text"])

            num_intervals = int(audio_duration / interval_seconds) + 1
            for i in range(num_intervals):
                t_start, t_end = i * interval_seconds, (i + 1) * interval_seconds
                transcripts, seen = [], set()

                for seg in transcription["segments"]:
                    seg_start, seg_end, text = seg.get("start", 0), seg.get("end", 0), seg.get("text", "").strip()

                    if max(t_start, seg_start) < min(t_end, seg_end):  # Overlapping transcript segment
                        speaker = "Unknown"

                        if include_speaker and diarization_df is not None:
                            for _, d_seg in diarization_df.iterrows():
                                d_start, d_end, detected_speaker = d_seg["start"], d_seg["end"], d_seg["speaker"].replace("#", "Person")
                                if max(seg_start, d_start) < min(seg_end, d_end) and (detected_speaker, int(seg_start), int(seg_end), text) not in seen:
                                    seen.add((detected_speaker, int(seg_start), int(seg_end), text))
                                    speaker = detected_speaker
                                    break

                        if include_speaker:
                            writer.writerow([
                                str(timedelta(seconds=t_start)),
                                str(timedelta(seconds=t_end)),
                                speaker,
                                str(timedelta(seconds=int(seg_start))),
                                str(timedelta(seconds=int(seg_end))),
                                text
                            ])
                        else:
                            writer.writerow([
                                str(timedelta(seconds=t_start)),
                                str(timedelta(seconds=t_end)),
                                str(timedelta(seconds=int(seg_start))),
                                str(timedelta(seconds=int(seg_end))),
                                text
                            ])

        # Step 8: Upload Transcript to Azure Blob Storage
        transcript_blob_url = blob.upload_file(transcript_filename, transcript_local_path, "transcripts")

        # Step 9: Update PostgreSQL with Transcript URL
        db.update_record("transcription_jobs", "id=%s", {
            "job_status": "completed",
            "transcript_blob_url": transcript_blob_url,
            "transcript_filename": transcript_filename
        }, (job_id,))

        logging.info(f"✅ Transcript saved and uploaded: {transcript_blob_url}")

    except Exception as e:
        logging.error(f"(process_transcription_job): Failed processing job {job_id}: {str(e)}")

@celery_app.task
def process_transcription_job(job_id, audio_blob_url, interval):
    """
    Background task to process the audio file transcript based on the given interval.
    Once complete, it generates a CSV transcript and updates the transcription_jobs table.
    """
    try:
        transcript_data = transcribe_audio_interval(audio_blob_url, interval)
        csv_filename = f"transcript_{job_id}.csv"
        csv_file_path = generate_csv_interval(transcript_data, csv_filename)
        transcript_blob_url = blob.upload_file(csv_filename, csv_file_path)

        # Update job record in the transcription_jobs table
        db.update_record("transcription_jobs", "id=%s", {
                "job_status": "completed",
                "transcript_blob_url": transcript_blob_url,
                "transcript_filename": csv_filename
            }, (job_id,))
    except Exception as e:
        raise Exception(f"(process_transcription_job): Failed processing job {job_id}: {str(e)}")



def generate_csv_interval(transcript_data, csv_filename):
    """
    Generates a CSV file from transcript data.
    transcript_data is expected to be a list of tuples: (time_interval, text)
    """
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Time Interval", "Transcript"])
        for interval, text in transcript_data:
            writer.writerow([interval, text])
    return csv_filename

def transcribe_audio_interval(audio_url, interval):
    """
    Call your Hugging Face model API (or your transcription service) here 
    to segment the audio file based on `interval` (e.g., 1min, 5min) and return transcript data.
    For demonstration we simulate a delay and dummy transcript.
    """
    # Simulated processing delay for large files
    time.sleep(5)  # in production, this will be your actual processing time
    if interval == "1min":
        transcript_data = [("00:00-01:00", "Transcript for first minute segment..."),
                           ("01:00-02:00", "Transcript for second minute segment...")]
    elif interval == "5min":
        transcript_data = [("00:00-05:00", "Transcript for first 5 minutes segment...")]
    else:
        transcript_data = [("00:00", "No transcript available")]
    return transcript_data