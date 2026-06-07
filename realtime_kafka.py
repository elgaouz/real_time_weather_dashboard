"""
Live weather snapshot from Kafka (same path as OpenWeather → producer → Kafka).
Updates a thread-safe cache so Dash can refresh every second without waiting for Spark/Hive.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections import deque
from typing import Any, Dict, List, Optional

from time_utils import now_morocco_str

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sample_topic")
MAX_TABLE_ROWS = int(os.getenv("REALTIME_TABLE_ROWS", "200"))

_lock = threading.Lock()
_latest_by_city: Dict[str, Dict[str, Any]] = {}
_recent: deque = deque(maxlen=MAX_TABLE_ROWS)
_last_poll_utc: Optional[str] = None
_messages_seen = 0
_consumer_error: Optional[str] = None
_stop = threading.Event()
_thread: Optional[threading.Thread] = None


def _apply_record(payload: Dict[str, Any], received_wallclock: str) -> None:
    global _last_poll_utc, _messages_seen
    city = str(payload.get("CityName", ""))
    if not city:
        return
    row = {
        "CityName": city,
        "Temperature": payload.get("Temperature"),
        "Humidity": payload.get("Humidity"),
        "CreationTime": str(payload.get("CreationTime", "")),
        "CreationDate": str(payload.get("CreationDate", "")),
        "DashboardReceivedAt": received_wallclock,
    }
    if not row["CreationDate"] and row["CreationTime"]:
        row["CreationDate"] = row["CreationTime"][:10]
    with _lock:
        _latest_by_city[city] = row
        _recent.appendleft(row)
        _last_poll_utc = received_wallclock
        _messages_seen += 1


def _loop() -> None:
    global _consumer_error
    try:
        from kafka import KafkaConsumer
    except ImportError:
        _consumer_error = "kafka-python not installed"
        return

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=f"dash-realtime-{uuid.uuid4().hex[:12]}",
            consumer_timeout_ms=1000,
        )
    except Exception as e:
        _consumer_error = str(e)
        return

    while not _stop.is_set():
        try:
            batches = consumer.poll(timeout_ms=800)
            now = now_morocco_str()
            for _tp, records in batches.items():
                for rec in records:
                    if isinstance(rec.value, dict):
                        _apply_record(rec.value, now)
            _consumer_error = None
        except Exception as e:
            _consumer_error = str(e)
            time.sleep(1.0)


def start_realtime_consumer() -> None:
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, name="kafka-realtime", daemon=True)
    _thread.start()


def get_snapshot() -> Dict[str, Any]:
    """Return latest-by-city rows, recent table rows, and status metadata."""
    with _lock:
        latest_rows = list(_latest_by_city.values())
        recent = list(_recent)
        seen = _messages_seen
        last = _last_poll_utc
    latest_rows.sort(key=lambda r: str(r.get("CityName", "")))
    return {
        "latest_rows": latest_rows,
        "table_rows": recent[:MAX_TABLE_ROWS],
        "messages_seen": seen,
        "last_event_local": last,
        "consumer_error": _consumer_error,
    }
