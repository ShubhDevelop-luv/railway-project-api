class History:
    """
    Model for retrieving transcripts based on date.
    """
    def __init__(self, user_id, date, transcripts):
        self.user_id = user_id
        self.date = date
        self.transcripts = transcripts