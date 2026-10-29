import numpy as np
import soundfile as sf


class CrowdReader:
    """Lightweight per-pipeline read cursor over a shared crowd buffer.

    The underlying ndarray is owned by a single CrowdLoop and read concurrently
    by multiple pipelines; each pipeline has its own `_pos` so cursors advance
    independently without a lock.
    """

    def __init__(self, buf: np.ndarray):
        self._buf = buf
        self._pos = 0
        self._n = len(buf)

    def read(self, frames: int) -> np.ndarray:
        out = np.empty((frames, self._buf.shape[1]), dtype=np.float32)
        written = 0
        while written < frames:
            chunk = min(frames - written, self._n - self._pos)
            out[written:written + chunk] = self._buf[self._pos:self._pos + chunk]
            self._pos = (self._pos + chunk) % self._n
            written += chunk
        return out


class CrowdLoop:
    """Seamless looping crowd ambience with a short equal-power crossfade
    at the loop boundary to avoid audible clicks."""

    def __init__(self, path: str, sample_rate: int, channels: int, crossfade_ms: int = 50, skip_sec: float = 0.0):
        data, sr = sf.read(path, dtype="float32", always_2d=True)
        
        # Resample if the file's sample rate doesn't match the pipeline
        if sr != sample_rate:
            old_len = len(data)
            new_len = int(old_len * sample_rate / sr)
            old_indices = np.arange(old_len)
            new_indices = np.linspace(0, old_len - 1, new_len)
            resampled = np.column_stack([
                np.interp(new_indices, old_indices, data[:, ch])
                for ch in range(data.shape[1])
            ]).astype(np.float32)
            data = resampled
            
        if skip_sec > 0.0:
            skip_frames = int(skip_sec * sample_rate)
            if skip_frames >= len(data):
                raise ValueError("skip_sec is larger than the file duration")
            data = data[skip_frames:]
            
        if data.shape[1] == 1 and channels == 2:
            data = np.repeat(data, 2, axis=1)
        self._buf = self._apply_loop_crossfade(data, int(sample_rate * crossfade_ms / 1000))
        self._pos = 0
        self._n = len(self._buf)

    @staticmethod
    def _apply_loop_crossfade(buf: np.ndarray, xfade: int) -> np.ndarray:
        if xfade <= 0 or xfade * 2 >= len(buf):
            return buf
        head = buf[:xfade].copy()
        tail = buf[-xfade:].copy()
        t = np.linspace(0, 1, xfade, dtype=np.float32)[:, None]
        fade_in = np.sin(t * np.pi / 2)
        fade_out = np.cos(t * np.pi / 2)
        merged = tail * fade_out + head * fade_in
        
        # Modify the buffer in-place to save memory (avoiding a ~640MB copy)
        buf = buf[:-xfade]
        buf[:xfade] = merged
        return buf

    def reader(self) -> CrowdReader:
        """Return a cheap per-pipeline cursor over the shared decoded buffer."""
        return CrowdReader(self._buf)

    def read(self, frames: int) -> np.ndarray:
        out = np.empty((frames, self._buf.shape[1]), dtype=np.float32)
        written = 0
        while written < frames:
            chunk = min(frames - written, self._n - self._pos)
            out[written:written + chunk] = self._buf[self._pos:self._pos + chunk]
            self._pos = (self._pos + chunk) % self._n
            written += chunk
        return out
