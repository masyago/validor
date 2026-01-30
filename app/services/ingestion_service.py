class IngestionService:
    def __init__(self):
        self.raw_repo = raw_repo
        self.ingestion_repo = ingestion_repo
        self.panel_repo = panel_repo
        self.test_repo = test_repo

    def process_ingestion(self):
        if not self.ingestion_repo.claim_for_processing(ingestion_id):
            return  # already claimed or not in a processable state
