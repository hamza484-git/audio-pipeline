import json
import logging
import threading
from typing import Callable, Optional

from google.cloud import pubsub_v1

from config import PubSubConfig
from scheduler import Delivery

log = logging.getLogger(__name__)


class PubSubDeliveryFeed:
    """Subscribes to a Google Cloud Pub/Sub topic and routes incoming
    commentary.audio_ready events to per-match delivery callbacks.

    The router creates a single Pub/Sub pull subscription and dispatches
    each message to the correct match handler via the on_delivery callback,
    which is keyed on match_id.
    """

    def __init__(
        self,
        config: PubSubConfig,
        on_delivery: Callable[[str, Delivery], None],
        match_id_filter: Optional[str] = None,
    ):
        """
        Args:
            config: Pub/Sub configuration (project_id, subscription_id).
            on_delivery: Callback invoked with (match_id, Delivery) for each
                         valid commentary.audio_ready event.
            match_id_filter: If set, only events for this match_id are
                             forwarded. If None, all match events are routed.
        """
        self._config = config
        self._on_delivery = on_delivery
        self._match_id_filter = match_id_filter
        self._subscriber = pubsub_v1.SubscriberClient()
        self._subscription_path = self._subscriber.subscription_path(
            config.project_id, config.subscription_id
        )
        self._streaming_pull_future = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Begin pulling messages from Pub/Sub in the background."""
        log.info(
            "Starting Pub/Sub listener on subscription: %s",
            self._subscription_path,
        )
        self._streaming_pull_future = self._subscriber.subscribe(
            self._subscription_path, callback=self._handle_message
        )

    def stop(self) -> None:
        """Gracefully shut down the Pub/Sub subscriber."""
        if self._streaming_pull_future:
            self._streaming_pull_future.cancel()
            try:
                self._streaming_pull_future.result(timeout=5)
            except Exception:
                pass
        self._subscriber.close()
        log.info("Pub/Sub listener stopped.")

    def _handle_message(self, message: pubsub_v1.types.PubsubMessage) -> None:
        """Process a single incoming Pub/Sub message."""
        try:
            data = json.loads(message.data.decode("utf-8"))

            # Only handle commentary.audio_ready events
            event_type = data.get("event_type", "")
            if event_type != "commentary.audio_ready":
                log.debug("Ignoring event type: %s", event_type)
                message.ack()
                return

            payload = data.get("data", {})
            match_id = data.get("match_id") or payload.get("match_id")
            commentary_id = data.get("commentary_id")
            storage_path = payload.get("storage_path") or payload.get("audio_url")
            duration_ms = payload.get("duration_ms", 5000)

            if not commentary_id or not storage_path:
                log.warning("Malformed Pub/Sub message, missing fields: %s", data)
                message.ack()
                return

            # Filter to specific match if configured
            if self._match_id_filter and match_id != self._match_id_filter:
                log.debug(
                    "Skipping event for match %s (filter: %s)", match_id, self._match_id_filter
                )
                message.ack()
                return

            delivery = Delivery(
                timestamp=0.0,  # Play immediately; Pipeline.enqueue handles buffering
                delivery_id=str(commentary_id),
                audio_path=storage_path,
                duration=duration_ms / 1000.0,
            )

            log.info(
                "Received commentary.audio_ready | match=%s commentary=%s duration=%.1fs",
                match_id,
                commentary_id,
                delivery.duration,
            )

            self._on_delivery(match_id, delivery)
            message.ack()

        except Exception as e:
            log.error("Error processing Pub/Sub message: %s", e)
            message.nack()  # Nack so the message is redelivered
