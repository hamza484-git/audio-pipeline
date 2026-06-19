import argparse
import logging
import time
import os
import threading
from typing import Optional

from broadcast_clock import BroadcastClock
from broadcast_output import BroadcastOutput
from commentary_cache import CommentaryCache
from config import AudioConfig, BroadcastConfig, GCSConfig, SchedulerConfig, CloudSQLConfig
from crowd_loop import CrowdLoop
from delivery_feed import JsonDeliveryFeed
from cloudsql_feed import CloudSQLDeliveryFeed
from mixer import AudioMixer
from scheduler import CommentaryScheduler, Delivery

log = logging.getLogger("audio-pipeline")

class Pipeline:
    def __init__(
        self,
        audio_cfg: AudioConfig,
        gcs_cfg: GCSConfig,
        sched_cfg: SchedulerConfig,
        broadcast_cfg: BroadcastConfig,
        video_input: Optional[str] = None,
        dry_run: bool = False,
        local_commentary: Optional[str] = None,
    ):
        self.audio_cfg = audio_cfg
        self.sched_cfg = sched_cfg
        self.dry_run = dry_run

        self._schedule_time = 0.0
        self._play_time = 0.0
        self._playback_lock = threading.Lock()

        if audio_cfg.crowd_file:
            self.crowd = CrowdLoop(audio_cfg.crowd_file, audio_cfg.sample_rate, audio_cfg.channels)
        else:
            self.crowd = None

        self.mixer = AudioMixer(
            self.crowd,
            audio_cfg.sample_rate,
            audio_cfg.channels,
            crowd_volume=audio_cfg.crowd_volume,
            ducked_volume=audio_cfg.ducked_volume,
            fade_ms=audio_cfg.fade_ms,
        )
        self.cache = CommentaryCache(
            gcs_cfg.bucket,
            gcs_cfg.cache_dir,
            audio_cfg.sample_rate,
            audio_cfg.channels,
            local_dir=local_commentary,
        )
        self.scheduler = CommentaryScheduler(
            on_trigger=self._on_trigger,
            on_preload=self._on_preload,
            preload_count=gcs_cfg.preload_count,
            trigger_lead_ms=sched_cfg.trigger_lead_ms,
        )
        self.clock = BroadcastClock(audio_cfg.sample_rate)
        self.output = None if dry_run else BroadcastOutput(
            broadcast_cfg.output_url,
            audio_cfg.sample_rate,
            audio_cfg.channels,
            video_input=video_input,
            video_codec=broadcast_cfg.video_codec,
            audio_codec=broadcast_cfg.audio_codec,
            audio_bitrate=broadcast_cfg.audio_bitrate,
            container=broadcast_cfg.container,
        )

    def enqueue(self, delivery: Delivery) -> None:
        start_time = max(delivery.timestamp, self.clock.seconds, self._schedule_time)
        delivery.timestamp = start_time
        self._schedule_time = start_time + delivery.duration + 2.0
        self.scheduler.enqueue(delivery)

    def _on_preload(self, d: Delivery) -> None:
        log.info("preload %s @ %.2fs", d.delivery_id, d.timestamp)
        self.cache.preload(d.delivery_id, d.audio_path)

    def _on_trigger(self, d: Delivery) -> None:
        latency_ms = (self.clock.seconds - d.timestamp) * 1000
        log.info("trigger %s (latency %.1fms)", d.delivery_id, latency_ms)
        if latency_ms > self.sched_cfg.max_trigger_delay_ms:
            log.warning("trigger latency %.1fms exceeds budget", latency_ms)
        audio = self.cache.get(d.delivery_id, d.audio_path, timeout=0.5)
        if audio is None:
            log.error("commentary %s not ready, skipping", d.delivery_id)
            return

        with self._playback_lock:
            while self.clock.seconds < self._play_time:
                time.sleep(0.05)
            self.mixer.play_commentary(audio)
            actual_duration = len(audio) / self.audio_cfg.sample_rate
            self._play_time = self.clock.seconds + actual_duration + 2.0

        self.cache.evict(d.delivery_id)

    def run(self) -> None:
        block = self.audio_cfg.block_size
        sr = self.audio_cfg.sample_rate
        block_sec = block / sr
        next_tick = time.monotonic()

        try:
            while True:
                self.scheduler.tick(self.clock.seconds)
                pcm = self.mixer.render(block)
                if self.output is not None:
                    self.output.write(pcm)
                self.clock.advance(block)

                next_tick += block_sec
                sleep = next_tick - time.monotonic()
                if sleep > 0:
                    time.sleep(sleep)
                else:
                    next_tick = time.monotonic()
        finally:
            if self.output is not None:
                self.output.close()


class PubSubRouter:
    """Routes incoming Pub/Sub deliveries to per-match Pipeline instances.

    A single Pub/Sub subscriber receives events for ALL matches. This router
    dynamically creates and manages one isolated Pipeline per match_id,
    preventing any cross-match audio contamination.

    Each match outputs its own HLS stream to:
        hls_stream/{match_id}/stream.m3u8
    """

    def __init__(
        self,
        audio_cfg: AudioConfig,
        gcs_cfg: GCSConfig,
        sched_cfg: SchedulerConfig,
        base_output_dir: str,
        crowd_noise: str,
        dry_run: bool = False,
    ):
        self._audio_cfg = audio_cfg
        self._gcs_cfg = gcs_cfg
        self._sched_cfg = sched_cfg
        self._base_output_dir = base_output_dir
        self._crowd_noise = crowd_noise
        self._dry_run = dry_run

        self._pipelines: dict = {}
        self._pipeline_threads: dict = {}
        self._lock = threading.Lock()

    def route(self, match_id: str, delivery: Delivery) -> None:
        """Route a delivery to the correct match pipeline, creating it if needed."""
        with self._lock:
            if match_id not in self._pipelines:
                pipeline = self._create_pipeline(match_id)
                self._pipelines[match_id] = pipeline
                t = threading.Thread(
                    target=pipeline.run,
                    name=f"pipeline-{match_id}",
                    daemon=True,
                )
                self._pipeline_threads[match_id] = t
                t.start()
                log.info("Started new pipeline for match: %s", match_id)

        self._pipelines[match_id].enqueue(delivery)

    def _create_pipeline(self, match_id: str) -> Pipeline:
        """Create a fully configured Pipeline for the given match."""
        output_dir = os.path.join(self._base_output_dir, match_id)
        os.makedirs(output_dir, exist_ok=True)

        broadcast_cfg = BroadcastConfig(
            output_url=os.path.join(output_dir, "stream.m3u8"),
            container="hls",
        )

        # Per-match GCS cache subdirectory to avoid cache collisions between matches
        gcs_cfg = GCSConfig(
            bucket=self._gcs_cfg.bucket,
            prefix=match_id,
            cache_dir=os.path.join(self._gcs_cfg.cache_dir, match_id),
            preload_count=self._gcs_cfg.preload_count,
        )

        return Pipeline(
            self._audio_cfg,
            gcs_cfg,
            self._sched_cfg,
            broadcast_cfg,
            dry_run=self._dry_run,
        )

    def active_matches(self) -> list:
        with self._lock:
            return list(self._pipelines.keys())


def _build_audio_cfg(crowd_noise: str) -> AudioConfig:
    audio_cfg = AudioConfig()
    crowd_map = {
        "india": "assets/crowd_india.wav",
        "australia": "assets/crowd_australia.wav",
        "england": "assets/crowd_england.wav",
        "none": None,
    }
    audio_cfg.crowd_file = crowd_map.get(crowd_noise)
    return audio_cfg


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--deliveries", default=None, help="use local JSON file instead of Cloud SQL or Pub/Sub")
    p.add_argument("--video", default=None, help="video input URL or file (optional)")
    p.add_argument("--dry-run", action="store_true", help="skip ffmpeg output")
    p.add_argument("--local-commentary", default=None, help="local folder of commentary clips (skips GCS)")
    p.add_argument("--output-url", default=None, help="override the broadcast output URL (single-match mode only)")
    p.add_argument("--crowd-noise", choices=["india", "australia", "england", "none"], default="india", help="choose background crowd noise")
    p.add_argument("--pubsub", action="store_true", help="use Google Cloud Pub/Sub feed (multi-match mode)")
    p.add_argument("--match-id", default=None, help="filter Pub/Sub events to a specific match ID (optional)")
    args = p.parse_args()

    audio_cfg = _build_audio_cfg(args.crowd_noise)

    # ── Pub/Sub multi-match mode ────────────────────────────────────────────
    if args.pubsub:
        from config import PubSubConfig
        from pubsub_feed import PubSubDeliveryFeed

        pubsub_cfg = PubSubConfig(
            project_id=os.environ.get("PUBSUB_PROJECT_ID", PubSubConfig.project_id),
            subscription_id=os.environ.get("PUBSUB_SUBSCRIPTION_ID", PubSubConfig.subscription_id),
        )

        router = PubSubRouter(
            audio_cfg=audio_cfg,
            gcs_cfg=GCSConfig(),
            sched_cfg=SchedulerConfig(),
            base_output_dir="hls_stream",
            crowd_noise=args.crowd_noise,
            dry_run=args.dry_run,
        )

        feed = PubSubDeliveryFeed(
            config=pubsub_cfg,
            on_delivery=router.route,
            match_id_filter=args.match_id or os.environ.get("MATCH_ID"),
        )

        log.info(
            "Starting Pub/Sub multi-match router | project=%s subscription=%s match_filter=%s",
            pubsub_cfg.project_id,
            pubsub_cfg.subscription_id,
            args.match_id or os.environ.get("MATCH_ID") or "ALL",
        )
        feed.start()

        try:
            while True:
                time.sleep(10)
                log.info("Active matches: %s", router.active_matches())
        except KeyboardInterrupt:
            log.info("Pub/Sub router stopped by user")
        finally:
            feed.stop()
        return

    # ── Single-match mode (JSON file or Cloud SQL) ─────────────────────────
    broadcast_cfg = BroadcastConfig()
    if args.output_url:
        broadcast_cfg.output_url = args.output_url
    elif os.environ.get("OUTPUT_URL"):
        broadcast_cfg.output_url = os.environ.get("OUTPUT_URL")

    if os.environ.get("OUTPUT_CONTAINER"):
        broadcast_cfg.container = os.environ.get("OUTPUT_CONTAINER")

    pipeline = Pipeline(
        audio_cfg,
        GCSConfig(),
        SchedulerConfig(),
        broadcast_cfg,
        video_input=args.video,
        dry_run=args.dry_run,
        local_commentary=args.local_commentary,
    )

    feed = None
    if args.deliveries:
        log.info(f"Using JSON delivery feed: {args.deliveries}")
        for d in JsonDeliveryFeed(args.deliveries).load():
            pipeline.enqueue(d)
    else:
        cloudsql_cfg = CloudSQLConfig(
            instance_connection_name=os.environ.get("DB_CONNECTION_NAME", CloudSQLConfig.instance_connection_name),
            db_user=os.environ.get("DB_USER", CloudSQLConfig.db_user),
            db_pass=os.environ.get("DB_PASS", CloudSQLConfig.db_pass),
            db_name=os.environ.get("DB_NAME", CloudSQLConfig.db_name),
            match_id=os.environ.get("MATCH_ID", CloudSQLConfig.match_id),
        )
        log.info(f"Using Cloud SQL feed for match: {cloudsql_cfg.match_id}")
        feed = CloudSQLDeliveryFeed(cloudsql_cfg, on_delivery=pipeline.enqueue)
        feed.start()

    try:
        pipeline.run()
    except KeyboardInterrupt:
        log.info("Pipeline stopped by user")
    finally:
        if feed:
            feed.stop()

if __name__ == "__main__":
    main()
