import logging
import uuid

import redis
from celery import Celery

from app.config import settings
from app.database import SessionLocal
from app.models import EventRecord, EventResponse

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 300

celery_app = Celery(
    "pipeline",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)


@celery_app.task(name="process_event_side_effects")
def process_event_side_effects(event_id: str, event_type: str, user_id: str) -> dict:
    """Run downstream work for an event that has already been stored."""
    logger.info(
        "Processing side effects for event_id=%s event_type=%s user_id=%s",
        event_id,
        event_type,
        user_id,
    )

    db = SessionLocal()
    try:
        record = db.get(EventRecord, uuid.UUID(event_id))
        if record is None:
            logger.warning("Event %s was not found for side effects", event_id)
            return {"event_id": event_id, "status": "missing"}

        record.status = "processed"
        db.commit()
        db.refresh(record)
        cached = EventResponse.model_validate(record).model_dump_json()
    except Exception:
        db.rollback()
        logger.exception("Side effects failed for event %s", event_id)
        raise
    finally:
        db.close()

    cache = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        cache.setex(f"event:{event_id}", CACHE_TTL_SECONDS, cached)
    finally:
        cache.close()

    return {
        "event_id": event_id,
        "event_type": event_type,
        "user_id": user_id,
        "status": "processed",
    }
