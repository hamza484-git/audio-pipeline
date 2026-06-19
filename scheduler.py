import heapq
import threading
from dataclasses import dataclass, field
from typing import Callable, List, Optional


@dataclass(order=True)
class Delivery:
    timestamp: float
    delivery_id: str = field(compare=False)
    audio_path: str = field(compare=False)
    duration: float = field(default=0.0, compare=False)


class CommentaryScheduler:
    """Timestamp-driven priority queue. Triggers playback when the
    broadcast clock passes a delivery's video_timestamp."""

    def __init__(
        self,
        on_trigger: Callable[[Delivery], None],
        on_preload: Optional[Callable[[Delivery], None]] = None,
        preload_count: int = 3,
        trigger_lead_ms: int = 50,
    ):
        self._queue: List[Delivery] = []
        self._lock = threading.Lock()
        self._on_trigger = on_trigger
        self._on_preload = on_preload
        self._preload_count = preload_count
        self._lead = trigger_lead_ms / 1000.0
        self._preloaded: set = set()

    def enqueue(self, delivery: Delivery) -> None:
        with self._lock:
            heapq.heappush(self._queue, delivery)

    def tick(self, current_ts: float) -> None:
        with self._lock:
            for d in self._queue[: self._preload_count]:
                if d.delivery_id not in self._preloaded and self._on_preload:
                    self._on_preload(d)
                    self._preloaded.add(d.delivery_id)

            while self._queue and self._queue[0].timestamp <= current_ts + self._lead:
                d = heapq.heappop(self._queue)
                self._preloaded.discard(d.delivery_id)
                threading.Thread(target=self._on_trigger, args=(d,), daemon=True).start()

    def upcoming(self) -> List[Delivery]:
        with self._lock:
            return list(self._queue)
