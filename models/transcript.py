class Transcript:
    """
    Model for storing transcription jobs in PostgreSQL.
    """
    def __init__(self, id, audio_file_id, interval, job_status, transcript_blob_url, transcript_filename, created_at, updated_at):
        self.id = id
        self.audio_file_id = audio_file_id
        self.interval = interval
        self.job_status = job_status
        self.transcript_blob_url = transcript_blob_url
        self.transcript_filename = transcript_filename
        self.created_at = created_at
        self.updated_at = updated_at