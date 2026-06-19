# Live Broadcast Audio Pipeline

Production-style audio pipeline for a live cricket broadcast:

- continuous, seamlessly looped **crowd ambience**
- timestamp-scheduled **AI commentary** clips from GCS
- automatic **ducking** of crowd under commentary
- mixed PCM piped to **ffmpeg** for RTMP / SRT / HLS publishing

## Layout

```
audio pipeline/
    assets/                 # crowd_idle.wav etc. (you supply)
    .cache/commentary/      # local GCS cache (auto-created)
    config.py               # all tunables
    crowd_loop.py           # seamless crossfade loop
    commentary_cache.py     # GCS download + decode + preload
    scheduler.py            # timestamp priority queue
    mixer.py                # ducking + commentary mix
    broadcast_clock.py      # sample-accurate broadcast time
    broadcast_output.py     # ffmpeg RTMP/SRT/HLS pipe
    delivery_feed.py        # JSON / push delivery source
    main.py                 # orchestrator
    deliveries.sample.json  # demo feed
    requirements.txt
```

## Components

| Component | Role |
|-----------|------|
| `CrowdLoop` | Reads `crowd_idle.wav` once, applies an equal-power crossfade across the loop boundary, then serves frames forever. |
| `CommentaryCache` | Async-downloads MP3s from `gs://<bucket>/<audio_path>`, decodes to float32 at the pipeline sample rate, keeps decoded clips in memory until consumed. |
| `CommentaryScheduler` | Min-heap keyed by `video_timestamp`. On each tick: preloads the next N clips, triggers playback once the broadcast clock crosses the timestamp (with a small lead). |
| `AudioMixer` | Pulls crowd frames every block, sums any active commentary clips on top, applies a linear gain ramp toward the target (100% → 30% on commentary start, 30% → 100% when none active). |
| `BroadcastClock` | Sample-accurate clock advanced by frames consumed; immune to scheduler jitter. |
| `BroadcastOutput` | Spawns `ffmpeg`, pipes raw `f32le` audio to stdin, optionally muxes a video input, publishes to the configured URL. |

## Setup

```powershell
pip install -r requirements.txt
# place a stereo 48 kHz crowd loop here:
#   assets/crowd_idle.wav
# auth to GCS:
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\path\to\sa.json"
```

## Run (dry, no ffmpeg)

```powershell
python main.py --deliveries deliveries.sample.json --dry-run
```

## Run (broadcast)

```powershell
python main.py `
    --deliveries deliveries.sample.json `
    --video "rtmp://ingest/source"
```

Override the RTMP target in `config.BroadcastConfig.output_url`.

## Delivery feed format

```json
{
  "delivery_id": "12345",
  "match_id": "1001",
  "video_timestamp": 752.25,
  "audio_path": "match_1001/delivery_12345.mp3",
  "duration": 4.5
}
```

`audio_path` is the object key inside the bucket configured in `GCSConfig.bucket`.

## Latency budget

- Scheduler tick: 20 ms (`SchedulerConfig.tick_ms`)
- Trigger lead: 50 ms (compensates for block boundary alignment)
- Hard ceiling: 500 ms — logs a warning if exceeded
- Preload window: next 3 deliveries by default

## Real-time delivery feed

Swap `JsonDeliveryFeed` for `PushDeliveryFeed` (Pub/Sub subscriber, Firestore listener, etc.) and call `pipeline.enqueue(delivery)` as events arrive. The scheduler accepts late inserts safely.

## Future hooks

`AudioMixer.play_commentary` is a generic clip injector — boundary stingers, wicket cheers, sponsor jingles all map onto the same path. Add per-clip target-gain overrides to express "don't duck for stingers" without changing the mixer's structure.
