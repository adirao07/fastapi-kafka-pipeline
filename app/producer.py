import json
import logging
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from app.config import settings

logger = logging.getLogger(__name__)


def _json_serializer(value: Any) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


def _key_serializer(key: str | None) -> bytes | None:
    if key is None:
        return None
    return key.encode("utf-8")


def create_producer(max_retries: int = 30, retry_interval: float = 2.0) -> KafkaProducer:
    """Connect to Kafka, waiting through broker startup."""
    last_error: Exception | None = None
    bootstrap_servers = [
        server.strip()
        for server in settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
        if server.strip()
    ]

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=_json_serializer,
                key_serializer=_key_serializer,
                acks="all",
                retries=5,
                api_version_auto_timeout_ms=5000,
            )
            logger.info("Connected to Kafka at %s", settings.KAFKA_BOOTSTRAP_SERVERS)
            return producer
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


def publish_event(producer: KafkaProducer, key: str, value: dict[str, Any]) -> None:
    future = producer.send(settings.KAFKA_TOPIC, key=key, value=value)
    metadata = future.get(timeout=10)
    logger.info(
        "Published %s to %s partition %s offset %s",
        key,
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )
