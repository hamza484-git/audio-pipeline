from dataclasses import dataclass


@dataclass
class AudioConfig:
    sample_rate: int = 48000
    channels: int = 2
    block_size: int = 1024
    dtype: str = "float32"

    crowd_file: str = "assets/crowd_idle.wav"
    crowd_volume: float = 1.0
    ducked_volume: float = 0.3
    fade_ms: int = 400


@dataclass
class GCSConfig:
    bucket: str = "commentary"
    prefix: str = "match_1001"
    cache_dir: str = ".cache/commentary"
    preload_count: int = 3


@dataclass
class SchedulerConfig:
    tick_ms: int = 20
    trigger_lead_ms: int = 50
    max_trigger_delay_ms: int = 500


@dataclass
class BroadcastConfig:
    output_url: str = "rtmp://localhost/live/stream"
    video_codec: str = "libx264"
    audio_codec: str = "aac"
    audio_bitrate: str = "192k"
    container: str = "flv"

@dataclass
class CloudSQLConfig:
    instance_connection_name: str = "project:region:instance"
    db_user: str = "postgres"
    db_pass: str = "password"
    db_name: str = "audio_db"
    match_id: str = "match_1001"
    poll_interval_sec: float = 1.0
