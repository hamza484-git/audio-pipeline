import shutil
import subprocess
from typing import Optional

import numpy as np


class BroadcastOutput:
    """Pipes mixed PCM audio (and optionally a video feed) into ffmpeg for
    RTMP / SRT / HLS publishing."""

    def __init__(
        self,
        output_url: str,
        sample_rate: int,
        channels: int,
        video_input: Optional[str] = None,
        video_codec: str = "libx264",
        audio_codec: str = "aac",
        audio_bitrate: str = "192k",
        container: str = "flv",
    ):
        if shutil.which("ffmpeg") is None:
            raise RuntimeError("ffmpeg not found on PATH")

        cmd = ["ffmpeg", "-loglevel", "warning", "-y"]
        if video_input:
            cmd += ["-re", "-i", video_input]
        cmd += [
            "-f", "f32le",
            "-ar", str(sample_rate),
            "-ac", str(channels),
            "-i", "pipe:0",
        ]
        if video_input:
            cmd += ["-map", "0:v:0", "-map", "1:a:0", "-c:v", video_codec]
        else:
            cmd += ["-map", "0:a:0"]
        cmd += [
            "-c:a", audio_codec,
            "-b:a", audio_bitrate,
            "-f", container,
            output_url,
        ]

        self._proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

    def write(self, pcm: np.ndarray) -> None:
        if self._proc.stdin is None:
            raise RuntimeError("ffmpeg stdin closed")
        self._proc.stdin.write(pcm.astype(np.float32).tobytes())

    def close(self) -> None:
        if self._proc.stdin:
            self._proc.stdin.close()
        self._proc.wait(timeout=5)
