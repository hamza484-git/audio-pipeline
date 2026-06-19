import threading
from dataclasses import dataclass
from typing import List

import numpy as np

from crowd_loop import CrowdLoop


@dataclass
class ActiveClip:
    audio: np.ndarray
    pos: int = 0

    def done(self) -> bool:
        return self.pos >= len(self.audio)


class AudioMixer:
    """Pulls crowd ambience continuously; mixes any active commentary clips
    over it with automatic ducking and equal-power fades."""

    def __init__(
        self,
        crowd: CrowdLoop,
        sample_rate: int,
        channels: int,
        crowd_volume: float = 1.0,
        ducked_volume: float = 0.3,
        fade_ms: int = 400,
    ):
        self._crowd = crowd
        self._sr = sample_rate
        self._ch = channels
        self._full = crowd_volume
        self._duck = ducked_volume
        self._fade_frames = max(1, int(sample_rate * fade_ms / 1000))

        self._lock = threading.Lock()
        self._clips: List[ActiveClip] = []
        self._current_gain = crowd_volume
        self._target_gain = crowd_volume

    def play_commentary(self, audio: np.ndarray) -> None:
        if audio.ndim == 1:
            audio = audio[:, None]
        if audio.shape[1] != self._ch:
            if audio.shape[1] == 1:
                audio = np.repeat(audio, self._ch, axis=1)
            else:
                audio = audio.mean(axis=1, keepdims=True).repeat(self._ch, axis=1)
        with self._lock:
            self._clips.append(ActiveClip(audio=audio.astype(np.float32)))

    def render(self, frames: int) -> np.ndarray:
        crowd = self._crowd.read(frames)

        with self._lock:
            active = self._clips
            self._target_gain = self._duck if active else self._full

            gain_ramp = self._build_gain_ramp(frames)
            out = crowd * gain_ramp[:, None]

            still_active: List[ActiveClip] = []
            for clip in active:
                remaining = len(clip.audio) - clip.pos
                take = min(remaining, frames)
                out[:take] += clip.audio[clip.pos:clip.pos + take]
                clip.pos += take
                if not clip.done():
                    still_active.append(clip)
            self._clips = still_active

        return np.clip(out, -1.0, 1.0)

    def _build_gain_ramp(self, frames: int) -> np.ndarray:
        if self._current_gain == self._target_gain:
            return np.full(frames, self._current_gain, dtype=np.float32)
        step = (self._target_gain - self._current_gain) / self._fade_frames
        ramp = self._current_gain + step * np.arange(1, frames + 1, dtype=np.float32)
        if step > 0:
            ramp = np.minimum(ramp, self._target_gain)
        else:
            ramp = np.maximum(ramp, self._target_gain)
        self._current_gain = float(ramp[-1])
        return ramp
