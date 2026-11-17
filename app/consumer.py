import json
import logging
import time
import uuid

import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from sqlalchemy.exc import IntegrityError

from app.celery_worker import process_event_side_effects
from app.config import settings
from app.database import Base, SessionLocal, engine
from app.models import EventRecord, EventResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 300
POLL_TIMEOUT_MS = 1000


def create_consumer(max_retries: int = 30, retry_interval: float = 2.0) -> KafkaConsumer:
    last_error: Exception | None = None
    bootstrap_servers = [
        server.strip()
        for server in settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
        if server.strip()
    ]

    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=bootstrap_servers,
                group_id="event-pipeline-consumers",
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                api_version_auto_timeout_ms=5000,
            )
            logger.info("Kafka consumer subscribed to %s", settings.KAFKA_TOPIC)
            return consumer
        except (NoBrokersAvailable, KafkaError) as exc:
            last_error = exc
            logger.warning(
                "Kafka not ready (attempt %s/%s): %s",
                attempt,
                max_retries,
                exc,
            )
            time.sleep(retry_interval)

    raise RuntimeError("Kafka broker unavailable after retries") from last_error


def handle_event(event: dict, cache: redis.Redis) -> None:
    required = ("event_id", "event_type", "user_id")
    if not all(event.get(field) for field in required):
        logger.error("Skipping malformed event: %s", event)
        return

    try:
        event_id = uuid.UUID(str(event["event_id"]))
    except ValueError:
        logger.error("Skipping event with invalid event_id: %s", event.get("event_id"))
        return

    event_type = str(event["event_type"])
    user_id = str(event["user_id"])
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        logger.error("Skipping event %s with a non-object payload", event_id)
        return

    db = SessionLocal()
    try:
        record = EventRecord(
            event_id=event_id,
            event_type=event_type,
            user_id=user_id,
            payload=payload,
            status="received",
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        response = EventResponse.model_validate(record)
    except IntegrityError:
        db.rollback()
        existing = db.get(EventRecord, event_id)
        if existing is None:
            raise
        response = EventResponse.model_validate(existing)
        logger.info("Event %s already stored; refreshing cache and side effects", event_id)
    except Exception:
        db.rollback()
        logger.exception("Failed to persist event %s", event_id)
        raise
    finally:
        db.close()

    cache.setex(f"event:{event_id}", CACHE_TTL_SECONDS, response.model_dump_json())
    process_event_side_effects.delay(str(event_id), event_type, user_id)
    logger.info("Stored event %s and queued side effects", event_id)


def main() -> None:
    Base.metadata.create_all(bind=engine)
    cache = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    consumer = create_consumer()
    logger.info("Consumer polling %s", settings.KAFKA_TOPIC)

    try:
        while True:
            try:
                batches = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            except Exception:
                logger.exception("Kafka poll failed")
                time.sleep(1)
                continue
            for messages in batches.values():
                for message in messages:
                    if not isinstance(message.value, dict):
                        logger.error("Skipping non-object message at offset %s", message.offset)
                        continue
                    try:
                        handle_event(message.value, cache)
                    except Exception:
                        logger.exception("Failed to handle message at offset %s", message.offset)
    except KeyboardInterrupt:
        logger.info("Consumer shutting down")
    finally:
        consumer.close()
        cache.close()


if __name__ == "__main__":
    main()
