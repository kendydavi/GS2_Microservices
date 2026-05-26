import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import List, Optional

import pika
import redis as redis_lib
from fastapi import FastAPI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Solar Shield - Alert Service")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
QUEUE_NAME = "space_weather_events"
CACHE_KEY = "alerts:all"
CACHE_TTL = 300  # 5 minutes — GST data from NASA updates at most every few hours

redis_client = redis_lib.from_url(REDIS_URL, decode_responses=True)

_alerts: List[dict] = []
_processed_ids: set = set()
_lock = threading.Lock()


# --- Business Rules ---

def classify_severity(kp_index: float) -> dict:
    """RN1: Classify geomagnetic storm severity based on Kp index."""
    if kp_index <= 4:
        severity = "low"
    elif kp_index <= 7:
        severity = "moderate"
    else:
        severity = "severe"
    return {"severity": severity, "emergencyNotification": severity == "severe"}


def process_event(event: dict) -> Optional[dict]:
    """Process event with idempotency guard (RN3)."""
    event_id = event.get("event_id")
    if not event_id:
        logger.warning("Event missing event_id — skipping")
        return None

    with _lock:
        # RN3: discard duplicates
        if event_id in _processed_ids:
            logger.info(f"DUPLICATE EVENT DISCARDED: event_id={event_id}")
            return None

        _processed_ids.add(event_id)
        classification = classify_severity(event.get("kp_index", 0))

        alert = {
            "event_id": event_id,
            "event_type": event.get("event_type", "UNKNOWN"),
            "start_time": event.get("start_time", ""),
            "kp_index": event.get("kp_index", 0),
            **classification,
            "processed_at": datetime.utcnow().isoformat(),
        }
        _alerts.append(alert)
        redis_client.delete(CACHE_KEY)  # invalidate stale cache
        logger.info(
            f"Alert created: id={event_id} severity={alert['severity']} "
            f"emergency={alert['emergencyNotification']}"
        )
        return alert


# --- RabbitMQ Consumer ---

def _consumer_loop():
    while True:
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                try:
                    event = json.loads(body)
                    process_event(event)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as exc:
                    logger.error(f"Message processing error: {exc}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            logger.info("RabbitMQ consumer started")
            channel.start_consuming()
        except Exception as exc:
            logger.error(f"RabbitMQ consumer error: {exc} — retrying in 5s")
            time.sleep(5)


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=_consumer_loop, daemon=True)
    t.start()


# --- API Endpoints ---

@app.get("/alerts")
async def get_alerts():
    """Return all processed alerts. Uses Redis Cache-Aside pattern (TTL=300s)."""
    cached = redis_client.get(CACHE_KEY)
    if cached:
        logger.info("Cache HIT: alerts:all")
        return {"source": "cache", "total": len(json.loads(cached)), "alerts": json.loads(cached)}

    logger.info("Cache MISS: alerts:all")
    with _lock:
        snapshot = list(_alerts)
    redis_client.setex(CACHE_KEY, CACHE_TTL, json.dumps(snapshot))
    return {"source": "database", "total": len(snapshot), "alerts": snapshot}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "alert-service"}
