import time


class BroadcastClock:
    """Monotonic broadcast timeline in seconds. Advances by frames consumed
    so audio scheduling stays sample-accurate even if wall-clock drifts."""

    def __init__(self, sample_rate: int, start_offset: float = 0.0):
        self._sr = sample_rate
        self._frames = 0
        self._offset = start_offset
        self._wall_start = time.monotonic()

    def advance(self, frames: int) -> None:
        self._frames += frames

    @property
    def seconds(self) -> float:
        return self._offset + self._frames / self._sr

    @property
    def wall_drift(self) -> float:
        return (time.monotonic() - self._wall_start) - self._frames / self._sr
