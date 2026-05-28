import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

import certifi
import httpx
import pika
from fastapi import FastAPI, HTTPException, Query

# SSL_VERIFY can be set to "false" in environments with SSL inspection proxies
# (e.g., corporate/university networks that intercept HTTPS traffic)
SSL_VERIFY = os.getenv("SSL_VERIFY", "true").lower() != "false"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Solar Shield - Ingest Service")

NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")
NASA_GST_URL = "https://api.nasa.gov/DONKI/GST"
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = "space_weather_events"


async def fetch_nasa_gst(start_date: str, end_date: str, max_retries: int = 3) -> list:
    params = {"startDate": start_date, "endDate": end_date, "api_key": NASA_API_KEY}
    for attempt in range(max_retries):
        try:
            ssl_ctx = certifi.where() if SSL_VERIFY else False
            async with httpx.AsyncClient(timeout=30.0, verify=ssl_ctx) as client:
                response = await client.get(NASA_GST_URL, params=params)
                response.raise_for_status()
                logger.info(f"NASA DONKI fetched successfully on attempt {attempt + 1}")
                return response.json() or []
        except (httpx.HTTPError, httpx.TimeoutException) as exc:
            wait = 2 ** attempt
            if attempt < max_retries - 1:
                logger.warning(f"NASA API attempt {attempt + 1} failed ({exc}), retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                logger.error(f"NASA API unavailable after {max_retries} retries")
                raise HTTPException(status_code=503, detail=f"NASA API unavailable: {exc}")


def publish_events(events: list) -> int:
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    count = 0
    for event in events:
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(event),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        count += 1
    connection.close()
    return count


@app.post("/ingest/gst")
async def ingest_gst(
    start_date: str = Query(default=None),
    end_date: str = Query(default=None),
):
    if not start_date:
        start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.utcnow().strftime("%Y-%m-%d")

    raw_events = await fetch_nasa_gst(start_date, end_date)

    if not raw_events:
        return {"message": "No GST events found for the period", "published": 0}

    payloads = []
    for ev in raw_events:
        kp_values = [kp.get("kpIndex", 0) for kp in (ev.get("allKpIndex") or [])]
        max_kp = max(kp_values) if kp_values else 0
        payloads.append(
            {
                "event_id": ev.get("gstID", ""),
                "event_type": "GST",
                "start_time": ev.get("startTime", ""),
                "kp_index": max_kp,
                "raw_data": ev,
            }
        )

    published = publish_events(payloads)
    logger.info(f"Published {published} GST events to RabbitMQ")
    return {"message": f"Published {published} events", "published": published, "events": payloads}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "ingest-service"}
