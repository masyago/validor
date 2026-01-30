"""
File contains enums that used across the application
"""

from enum import Enum


class IngestionStatus(str, Enum):
    """
    Defines possible statuses of an ingestion process.
      - RECEIVED: The ingestion has been received and is queued to be processed.
      - PROCESSING: The ingestion is being processed.
      - COMPLETED: The ingestion has been successfully processed.
      - FAILED VALIDATION: The ingestion process failed due invalid input/schema.
      - FAILED: The ingestion process failed due to non-validation errors.
    """

    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED_VALIDATION = "FAILED VALIDATION"
    FAILED = "FAILED"
