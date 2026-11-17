import json
import logging
import uuid
from contextlib import asynccontextmanager

import redis
from fastapi import Depends, FastAPI, HTTPException, status
from kafka.errors import KafkaError
from sqlalchemy.orm import Session

from app.config import settings
from app.database import Base, engine, get_db
from app.models import EventCreate, EventRecord, EventResponse
from app.producer import create_producer, publish_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 300
producer = None
cache: redis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, cache
    Base.metadata.create_all(bind=engine)
    producer = create_producer()
    cache = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    yield
    if producer is not None:
        producer.flush(timeout=5)
        producer.close(timeout=5)
    if cache is not None:
        cache.close()


app = FastAPI(title="Event Pipeline", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/events", status_code=status.HTTP_202_ACCEPTED)
def create_event(body: EventCreate) -> dict[str, str]:
    if producer is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Event bus unavailable")

    event_id = str(uuid.uuid4())
    message = {
        "event_id": event_id,
        "event_type": body.event_type,
        "user_id": body.user_id,
        "payload": body.payload,
    }
    try:
        publish_event(producer, key=event_id, value=message)
    except KafkaError as exc:
        logger.exception("Failed to publish event %s", event_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event bus unavailable",
        ) from exc

    return {"event_id": event_id, "status": "accepted"}


@app.get("/api/v1/events/{event_id}", response_model=EventResponse)
def get_event(event_id: uuid.UUID, db: Session = Depends(get_db)) -> EventResponse:
    cache_key = f"event:{event_id}"
    if cache is not None:
        cached = cache.get(cache_key)
        if cached:
            return EventResponse.model_validate(json.loads(cached))

    record = db.get(EventRecord, event_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")

    response = EventResponse.model_validate(record)
    if cache is not None:
        cache.setex(cache_key, CACHE_TTL_SECONDS, response.model_dump_json())
    return response
