import os
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, Optional

import numpy as np
import soundfile as sf

try:
    from google.cloud import storage
except ImportError:
    storage = None


class CommentaryCache:
    """Downloads commentary clips from GCS, decodes to float32 numpy arrays,
    and serves them by delivery_id. Supports preloading upcoming clips."""

    def __init__(
        self,
        bucket: str,
        cache_dir: str,
        sample_rate: int,
        channels: int,
        workers: int = 4,
        local_dir: Optional[str] = None,
    ):
        self.bucket_name = bucket
        self.cache_dir = cache_dir
        self.sample_rate = sample_rate
        self.channels = channels
        self.local_dir = local_dir
        os.makedirs(cache_dir, exist_ok=True)

        self._client = None
        self._bucket = None

        self._pool = ThreadPoolExecutor(max_workers=workers)
        self._lock = threading.Lock()
        self._decoded: Dict[str, np.ndarray] = {}
        self._inflight: Dict[str, Future] = {}

    def preload(self, delivery_id: str, gcs_path: str) -> None:
        with self._lock:
            if delivery_id in self._decoded or delivery_id in self._inflight:
                return
            self._inflight[delivery_id] = self._pool.submit(self._fetch_and_decode, delivery_id, gcs_path)

    def get(self, delivery_id: str, gcs_path: Optional[str] = None, timeout: float = 1.0) -> Optional[np.ndarray]:
        with self._lock:
            if delivery_id in self._decoded:
                return self._decoded[delivery_id]
            fut = self._inflight.get(delivery_id)

        if fut is None:
            if gcs_path is None:
                return None
            self.preload(delivery_id, gcs_path)
            with self._lock:
                fut = self._inflight[delivery_id]

        try:
            return fut.result(timeout=timeout)
        except Exception:
            return None

    def evict(self, delivery_id: str) -> None:
        with self._lock:
            self._decoded.pop(delivery_id, None)
            self._inflight.pop(delivery_id, None)

    def _fetch_and_decode(self, delivery_id: str, gcs_path: str) -> np.ndarray:
        if self.local_dir:
            local = os.path.join(self.local_dir, os.path.basename(gcs_path))
            if not os.path.exists(local):
                raise FileNotFoundError(local)
        else:
            local = os.path.join(self.cache_dir, f"{delivery_id}.bin")
            if not os.path.exists(local):
                if storage is None:
                    raise RuntimeError("google-cloud-storage not installed")
                if self._bucket is None:
                    self._client = storage.Client()
                    self._bucket = self._client.bucket(self.bucket_name)
                blob = self._bucket.blob(gcs_path)
                blob.download_to_filename(local)

        data, sr = sf.read(local, dtype="float32", always_2d=True)
        if sr != self.sample_rate:
            data = self._resample(data, sr, self.sample_rate)
        if data.shape[1] == 1 and self.channels == 2:
            data = np.repeat(data, 2, axis=1)
        elif data.shape[1] == 2 and self.channels == 1:
            data = data.mean(axis=1, keepdims=True)

        with self._lock:
            self._decoded[delivery_id] = data
            self._inflight.pop(delivery_id, None)
        return data

    @staticmethod
    def _resample(data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
        ratio = dst_sr / src_sr
        n_out = int(len(data) * ratio)
        x_old = np.linspace(0, 1, len(data), dtype=np.float32)
        x_new = np.linspace(0, 1, n_out, dtype=np.float32)
        out = np.stack([np.interp(x_new, x_old, data[:, c]) for c in range(data.shape[1])], axis=1)
        return out.astype(np.float32)
