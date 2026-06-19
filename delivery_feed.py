import json
import threading
from typing import Callable, Iterable

from scheduler import Delivery


class JsonDeliveryFeed:
    """Loads delivery events from a JSON file. Replace with a GCP Pub/Sub
    or Firestore listener in production."""

    def __init__(self, path: str):
        self._path = path

    def load(self) -> Iterable[Delivery]:
        with open(self._path, "r", encoding="utf-8") as f:
            rows = json.load(f)
        for r in rows:
            yield Delivery(
                delivery_id=str(r["delivery_id"]),
                timestamp=float(r["video_timestamp"]),
                audio_path=r["audio_path"],
                duration=float(r.get("duration", 0.0)),
            )


class PushDeliveryFeed:
    """Push-based feed for real-time delivery events from upstream services."""

    def __init__(self, on_delivery: Callable[[Delivery], None]):
        self._on = on_delivery
        self._lock = threading.Lock()

    def push(self, delivery: Delivery) -> None:
        with self._lock:
            self._on(delivery)
