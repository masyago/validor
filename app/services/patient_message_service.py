from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.persistence.repositories.patient_message_repo import (
    PatientMessageRepository,
)
from app.persistence.repositories.patient_repo import PatientRepository
from app.persistence.repositories.processing_event_repo import (
    ProcessingEventRepository,
)
from app.persistence.models.patient_message import (
    PatientMessage,
    PatientMessageReviewStatus,
    PatientMessageValidationStatus,
)
from app.persistence.models.provenance import (
    ProcessingEventActor,
    ProcessingEventSeverity,
    ProcessingEventTargetType,
    ProcessingEventType,
)
from app.provenance.emitter import EventContext, emit


class PatientMessageError(Exception):
    """Base error for patient-message review operations."""


class PatientMessageNotFoundError(PatientMessageError):
    pass


class InvalidTransitionError(PatientMessageError):
    """A requested review-status transition is not allowed from the current
    state (maps to HTTP 409 at the API layer)."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PatientMessageService:
    """
    Human gate: clinician review + a demo-only "send".

    `review_status` (human) is kept separate from `validation_status` (machine).
    The machine gate must have ACCEPTED a draft before a clinician can act on it
    — enforced here, since only ACCEPTED drafts are advanced to PENDING_REVIEW.
    """

    def __init__(self, session: Session):
        self.session = session
        self.repo = PatientMessageRepository(session)
        self.patient_repo = PatientRepository(session)
        self.pe_repo = ProcessingEventRepository(session)

    def get(self, patient_message_id: UUID) -> PatientMessage:
        message = self.repo.get(patient_message_id)
        if message is None:
            raise PatientMessageNotFoundError(str(patient_message_id))
        return message

    def get_active_for_ingestion(
        self, ingestion_id: UUID
    ) -> PatientMessage | None:
        return self.repo.get_active_by_ingestion_id(ingestion_id)

    # ------------------------------------------------------------------
    # Human-gate transitions
    # ------------------------------------------------------------------

    def approve(
        self,
        patient_message_id: UUID,
        *,
        approved_by: str,
        final_content_json: Optional[dict[str, Any]] = None,
    ) -> PatientMessage:
        message = self.get(patient_message_id)
        if message.review_status not in (
            PatientMessageReviewStatus.PENDING_REVIEW,
            PatientMessageReviewStatus.CHANGES_REQUESTED,
        ):
            raise InvalidTransitionError(
                f"Cannot approve a message in {message.review_status.value}"
            )
        # Machine gate precedes the human gate.
        if message.validation_status != PatientMessageValidationStatus.ACCEPTED:
            raise InvalidTransitionError(
                "Cannot approve a message whose draft was not machine-accepted"
            )

        # Clinician-approved content: edited version if supplied, else the draft.
        message.final_content_json = (
            final_content_json
            if final_content_json is not None
            else message.draft_content_json
        )
        message.review_status = PatientMessageReviewStatus.APPROVED
        message.approved_by = approved_by
        message.reviewed_by = approved_by
        now = _now()
        message.approved_at = now
        message.reviewed_at = now
        self.session.flush()
        self.session.commit()
        return message

    def request_changes(
        self,
        patient_message_id: UUID,
        *,
        reviewed_by: str,
        note: str,
    ) -> PatientMessage:
        message = self.get(patient_message_id)
        if message.review_status != PatientMessageReviewStatus.PENDING_REVIEW:
            raise InvalidTransitionError(
                f"Cannot request changes from {message.review_status.value}"
            )
        message.review_status = PatientMessageReviewStatus.CHANGES_REQUESTED
        message.reviewed_by = reviewed_by
        message.reviewed_at = _now()
        message.review_note = note
        self.session.flush()
        self.session.commit()
        return message

    def reject(
        self,
        patient_message_id: UUID,
        *,
        reviewed_by: str,
        note: Optional[str] = None,
    ) -> PatientMessage:
        message = self.get(patient_message_id)
        if message.review_status in (
            PatientMessageReviewStatus.SENT,
            PatientMessageReviewStatus.SUPERSEDED,
        ):
            raise InvalidTransitionError(
                f"Cannot reject a message in {message.review_status.value}"
            )
        message.review_status = PatientMessageReviewStatus.REJECTED
        message.reviewed_by = reviewed_by
        message.reviewed_at = _now()
        if note is not None:
            message.review_note = note
        self.session.flush()
        self.session.commit()
        return message

    def send(self, patient_message_id: UUID) -> PatientMessage:
        """
        Demo-send: flips APPROVED -> SENT and stamps sent_at. This is a no-op;
        there is NO external delivery and no patient-facing surface. An audit
        event is recorded.
        """
        message = self.get(patient_message_id)
        if message.review_status != PatientMessageReviewStatus.APPROVED:
            raise InvalidTransitionError(
                f"Cannot send a message in {message.review_status.value}"
            )
        message.review_status = PatientMessageReviewStatus.SENT
        message.sent_at = _now()

        ctx = EventContext(
            ingestion_id=message.ingestion_id,
            actor=ProcessingEventActor.MESSAGE_DRAFTER,
        )
        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.MESSAGE_SENT,
            severity=ProcessingEventSeverity.INFO,
            message="Patient message sent (demo; no external delivery)",
            details={"patient_message_id": str(patient_message_id)},
            target_type=ProcessingEventTargetType.PATIENT_MESSAGE,
            target_id=patient_message_id,
            deduped=False,
        )
        self.session.flush()
        self.session.commit()
        return message
