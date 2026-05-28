"""
Unit tests covering RN1 (severity classification) and RN3 (idempotency).
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src/alert-service"))

from business_rules import classify_severity, process_event, _processed_ids, _alerts


def setup_function():
    """Reset shared state before each test."""
    _processed_ids.clear()
    _alerts.clear()


# --- RN1 Tests ---

def test_rn1_low_severity():
    """Kp index <= 4 must be classified as 'low' with no emergency notification."""
    result = classify_severity(4)
    assert result["severity"] == "low"
    assert result["emergencyNotification"] is False


def test_rn1_moderate_severity():
    """Kp index between 5 and 7 must be classified as 'moderate'."""
    result = classify_severity(6)
    assert result["severity"] == "moderate"
    assert result["emergencyNotification"] is False


def test_rn1_severe_with_emergency():
    """Kp index >= 8 must be 'severe' and trigger emergencyNotification = True."""
    result = classify_severity(9)
    assert result["severity"] == "severe"
    assert result["emergencyNotification"] is True


# --- RN3 Tests ---

def test_rn3_duplicate_event_discarded():
    """Second event with the same event_id must be discarded and logged."""
    event = {"event_id": "GST-2024-001", "event_type": "GST", "start_time": "2024-01-01", "kp_index": 6}

    first = process_event(event)
    second = process_event(event)  # duplicate

    assert first is not None, "First event should be processed"
    assert second is None, "Duplicate event must be discarded (RN3)"
    assert len(_alerts) == 1, "Only one alert should exist after duplicate"
