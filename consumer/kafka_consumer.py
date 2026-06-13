"""
consumer/kafka_consumer.py
───────────────────────────
Subscribes to the CDC Kafka topic, deserialises each message,
and delegates persistence to ADLSUploader.
"""

import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import get_logger, settings
from consumer.adls_uploader import ADLSUploader

logger = get_logger(__name__)


class CDCConsumer:
    """
    Long-running Kafka consumer.
    Each message == one CDC event dict → uploaded to ADLS Bronze.
    """

    def __init__(self):
        self._consumer: KafkaConsumer | None = None
        self._uploader = ADLSUploader()

    # ── Setup ─────────────────────────────────────────────────

    def connect(self) -> None:
        """Open the Kafka consumer and the ADLS uploader."""
        try:
            self._consumer = KafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_GROUP_ID,
                auto_offset_reset="earliest",   # read from beginning on first start
                enable_auto_commit=True,
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            logger.info(
                "Kafka consumer subscribed to topic '%s'.", settings.KAFKA_TOPIC
            )
        except KafkaError as exc:
            logger.error("Could not create Kafka consumer: %s", exc)
            raise

        self._uploader.connect()

    # ── Main loop ─────────────────────────────────────────────

    def run(self) -> None:
        """Poll Kafka and upload every message to ADLS."""
        self.connect()
        logger.info("Consumer running …  (Ctrl+C to stop)")
        try:
            for message in self._consumer:
                event: dict = message.value
                logger.info(
                    "Received from Kafka – partition=%s offset=%s event_id=%s op=%s",
                    message.partition,
                    message.offset,
                    event.get("event_id"),
                    event.get("operation"),
                )
                try:
                    self._uploader.upload_event(event)
                except Exception as exc:          # pylint: disable=broad-except
                    # Log but don't crash the consumer on a single bad upload
                    logger.error(
                        "Skipping event_id=%s due to upload error: %s",
                        event.get("event_id"), exc,
                    )

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user.")
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        if self._consumer:
            self._consumer.close()
            logger.info("Kafka consumer closed.")


# ──────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    CDCConsumer().run()
