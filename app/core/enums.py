"""
File contains enums that used across the application
"""

from enum import Enum


class IngestionStatus(str, Enum):
    """
    Defines possible statuses of an ingestion process.

      - PROCESSING: The ingestion has been received and being processed.
      - COMPLETED: The ingestion has been successfully processed.
      - FAILED VALIDATION: The ingestion process failed due invalid input/schema.
      - FAILED: The ingestion process failed due to non-validation errors.
    """

    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED_VALIDATION = "FAILED VALIDATION"
    FAILED = "FAILED"
