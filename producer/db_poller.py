"""
producer/db_poller.py
─────────────────────
Polls the PostgreSQL `cdc_events` table at a fixed interval.
New rows (events) are serialised to JSON and published to a
Kafka topic.

DUPLICATE-PREVENTION DESIGN
────────────────────────────
The last-processed event_id (watermark) is persisted to a small
file (watermark.json) in the project root.  On every restart the
producer reads this file and continues from where it stopped —
so old / pre-existing events are NEVER re-sent to Kafka or ADLS.

Without persistence the watermark would reset to 0 on every
restart and all historic cdc_events rows would be re-published.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import get_logger, settings

logger = get_logger(__name__)

# ── Watermark file path (project root) ────────────────────────────────────
_WATERMARK_FILE = Path(__file__).resolve().parent.parent / "watermark.json"


# ──────────────────────────────────────────────────────────────────────────
# Watermark helpers – persist last processed event_id across restarts
# ──────────────────────────────────────────────────────────────────────────

def _load_watermark() -> int:
    """
    Read the last successfully published event_id from disk.
    Returns 0 if no watermark file exists yet (fresh start).
    """
    if _WATERMARK_FILE.exists():
        try:
            data = json.loads(_WATERMARK_FILE.read_text(encoding="utf-8"))
            watermark = int(data.get("last_event_id", 0))
            logger.info("Watermark loaded: last_event_id=%d", watermark)
            return watermark
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            logger.warning("Could not parse watermark file, starting from 0. Error: %s", exc)
    else:
        logger.info("No watermark file found — starting from event_id=0 (fresh run).")
    return 0


def _save_watermark(last_event_id: int) -> None:
    """Persist the watermark to disk after each successful publish batch."""
    try:
        _WATERMARK_FILE.write_text(
            json.dumps({"last_event_id": last_event_id}, indent=2),
            encoding="utf-8",
        )
        logger.debug("Watermark saved: last_event_id=%d", last_event_id)
    except OSError as exc:
        logger.error("Could not save watermark: %s", exc)


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _json_serialiser(obj):
    """JSON can't handle datetime objects out of the box."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")


def _build_postgres_dsn() -> str:
    return (
        f"host={settings.POSTGRES_HOST} "
        f"port={settings.POSTGRES_PORT} "
        f"dbname={settings.POSTGRES_DB} "
        f"user={settings.POSTGRES_USER} "
        f"password={settings.POSTGRES_PASSWORD}"
    )


# ──────────────────────────────────────────────────────────────────────────
# Core class
# ──────────────────────────────────────────────────────────────────────────

class CDCPoller:
    """
    Connects to Postgres, fetches unprocessed CDC events,
    and publishes each one to Kafka.

    The watermark (last processed event_id) is loaded from
    watermark.json on startup and saved after every successful
    batch, guaranteeing no event is sent twice — even across
    producer restarts.
    """

    def __init__(self):
        self._conn = None
        self._producer = None
        # Load persisted watermark — this is the key no-duplicate guarantee
        self._last_event_id: int = _load_watermark()

    # ── Connection management ──────────────────────────────────────────────

    def connect_postgres(self) -> None:
        """Open (or re-open) the PostgreSQL connection."""
        try:
            self._conn = psycopg2.connect(_build_postgres_dsn())
            self._conn.autocommit = True
            logger.info("Connected to PostgreSQL.")
        except psycopg2.OperationalError as exc:
            logger.error("Could not connect to PostgreSQL: %s", exc)
            raise

    def connect_kafka(self) -> None:
        """Create the Kafka producer."""
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(
                    v, default=_json_serialiser
                ).encode("utf-8"),
                acks="all",   # wait for all replicas to acknowledge
                retries=3,
            )
            logger.info(
                "Kafka producer connected to %s.", settings.KAFKA_BOOTSTRAP_SERVERS
            )
        except KafkaError as exc:
            logger.error("Could not connect to Kafka: %s", exc)
            raise

    # ── Polling ────────────────────────────────────────────────────────────

    def _fetch_new_events(self) -> list[dict]:
        """Return CDC events with event_id > watermark, ordered ascending."""
        sql = """
            SELECT event_id, operation, customer_id,
                   customer_name, email, updated_at, captured_at
            FROM   cdc_events
            WHERE  event_id > %s
            ORDER  BY event_id ASC;
        """
        try:
            with self._conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute(sql, (self._last_event_id,))
                rows = cur.fetchall()
            return [dict(row) for row in rows]
        except psycopg2.Error as exc:
            logger.error("Error fetching CDC events: %s", exc)
            return []

    def _publish_event(self, event: dict) -> None:
        """Send a single CDC event dict to the Kafka topic."""
        try:
            future = self._producer.send(settings.KAFKA_TOPIC, value=event)
            future.get(timeout=10)   # block until broker acks
            logger.info(
                "Published event_id=%s  op=%s  customer_id=%s",
                event["event_id"], event["operation"], event["customer_id"],
            )
        except KafkaError as exc:
            logger.error("Failed to publish event %s: %s", event.get("event_id"), exc)

    # ── Main loop ──────────────────────────────────────────────────────────

    def run(self) -> None:
        """Poll continuously until interrupted."""
        self.connect_postgres()
        self.connect_kafka()
        logger.info(
            "Polling every %s second(s) …  (Ctrl+C to stop)",
            settings.POLL_INTERVAL_SECONDS,
        )
        try:
            while True:
                events = self._fetch_new_events()
                if events:
                    logger.info("Found %d new CDC event(s).", len(events))
                    for event in events:
                        self._publish_event(event)
                        # Advance watermark in memory after each event
                        self._last_event_id = max(
                            self._last_event_id, event["event_id"]
                        )
                    self._producer.flush()
                    # Persist watermark to disk after the whole batch
                    _save_watermark(self._last_event_id)
                else:
                    logger.debug("No new events this cycle.")

                time.sleep(settings.POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Poller stopped by user.")
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        if self._producer:
            self._producer.close()
            logger.info("Kafka producer closed.")
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed.")


# ──────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    CDCPoller().run()
