import logging
import threading
from datetime import datetime, timezone
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

_alerts: List[dict] = []
_processed_ids: set = set()
_lock = threading.Lock()


def classify_severity(kp_index: float) -> dict:
    """RN1: Classify geomagnetic storm severity based on Kp index."""
    if kp_index <= 4:
        severity = "low"
    elif kp_index <= 7:
        severity = "moderate"
    else:
        severity = "severe"
    return {"severity": severity, "emergencyNotification": severity == "severe"}


def process_event(event: dict, on_new_alert: Optional[Callable] = None) -> Optional[dict]:
    """Process event with idempotency guard (RN3)."""
    event_id = event.get("event_id")
    if not event_id:
        logger.warning("Event missing event_id — skipping")
        return None

    with _lock:
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
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        _alerts.append(alert)
        logger.info(
            f"Alert created: id={event_id} severity={alert['severity']} "
            f"emergency={alert['emergencyNotification']}"
        )

    if on_new_alert:
        on_new_alert()
    return alert
