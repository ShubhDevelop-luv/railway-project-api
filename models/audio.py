class Audio:
    """
    Model for storing uploaded audio files in PostgreSQL.
    """
    def __init__(self, id, user_id, filename, blob_url, status, uploaded_at):
        self.id = id
        self.user_id = user_id
        self.filename = filename
        self.blob_url = blob_url
        self.status = status
        self.uploaded_at = uploaded_at