"""
tests/test_pipeline.py
───────────────────────
Beginner-friendly unit tests for the CDC pipeline.
External services (Postgres, Kafka, ADLS) are mocked so the
tests run without any infrastructure.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ══════════════════════════════════════════════════════════════════
# 1.  Settings / Config
# ══════════════════════════════════════════════════════════════════

def test_settings_defaults():
    """Settings object should expose all required keys."""
    from config.settings import Settings
    s = Settings()
    assert hasattr(s, "POSTGRES_HOST")
    assert hasattr(s, "KAFKA_TOPIC")
    assert hasattr(s, "ADLS_CONTAINER_NAME")


def test_get_logger_returns_logger():
    """get_logger should return a standard logging.Logger."""
    import logging
    from config.logger import get_logger
    logger = get_logger("test_module")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_module"


# ══════════════════════════════════════════════════════════════════
# 2.  Watermark persistence (no-duplicate guarantee)
# ══════════════════════════════════════════════════════════════════

def test_load_watermark_returns_zero_when_no_file(tmp_path, monkeypatch):
    """_load_watermark should return 0 when watermark file does not exist."""
    import producer.db_poller as dp
    monkeypatch.setattr(dp, "_WATERMARK_FILE", tmp_path / "watermark.json")
    assert dp._load_watermark() == 0


def test_save_and_load_watermark_roundtrip(tmp_path, monkeypatch):
    """Saving then loading the watermark should return the same value."""
    import producer.db_poller as dp
    monkeypatch.setattr(dp, "_WATERMARK_FILE", tmp_path / "watermark.json")
    dp._save_watermark(42)
    assert dp._load_watermark() == 42


def test_load_watermark_handles_corrupt_file(tmp_path, monkeypatch):
    """_load_watermark should return 0 gracefully if the file is corrupt."""
    import producer.db_poller as dp
    wf = tmp_path / "watermark.json"
    wf.write_text("NOT_VALID_JSON", encoding="utf-8")
    monkeypatch.setattr(dp, "_WATERMARK_FILE", wf)
    assert dp._load_watermark() == 0


def test_watermark_advances_and_persists(tmp_path, monkeypatch):
    """After publishing events, watermark must be saved with the highest event_id."""
    import producer.db_poller as dp
    monkeypatch.setattr(dp, "_WATERMARK_FILE", tmp_path / "watermark.json")

    # Simulate two save calls (as the run loop would do)
    dp._save_watermark(5)
    dp._save_watermark(10)

    # Reload should give the latest value
    assert dp._load_watermark() == 10


# ══════════════════════════════════════════════════════════════════
# 3.  Producer – CDCPoller
# ══════════════════════════════════════════════════════════════════

class TestCDCPoller:

    @patch("producer.db_poller.psycopg2.connect")
    def test_connect_postgres_success(self, mock_connect):
        """connect_postgres should call psycopg2.connect exactly once."""
        from producer.db_poller import CDCPoller
        poller = CDCPoller()
        poller.connect_postgres()
        mock_connect.assert_called_once()

    @patch("producer.db_poller.psycopg2.connect", side_effect=Exception("conn refused"))
    def test_connect_postgres_raises_on_failure(self, mock_connect):
        """connect_postgres should propagate connection errors."""
        from producer.db_poller import CDCPoller
        poller = CDCPoller()
        with pytest.raises(Exception, match="conn refused"):
            poller.connect_postgres()

    @patch("producer.db_poller.KafkaProducer")
    def test_connect_kafka_success(self, mock_kafka):
        """connect_kafka should instantiate a KafkaProducer."""
        from producer.db_poller import CDCPoller
        poller = CDCPoller()
        poller.connect_kafka()
        mock_kafka.assert_called_once()

    def test_fetch_new_events_returns_list(self):
        """_fetch_new_events should return a list (possibly empty)."""
        from producer.db_poller import CDCPoller
        poller = CDCPoller()

        fake_rows = [
            {"event_id": 1, "operation": "INSERT", "customer_id": 10,
             "customer_name": "Alice", "email": "alice@test.com",
             "updated_at": datetime(2024, 1, 1), "captured_at": datetime(2024, 1, 1)},
            {"event_id": 2, "operation": "UPDATE", "customer_id": 10,
             "customer_name": "Alice B", "email": "alice@test.com",
             "updated_at": datetime(2024, 1, 2), "captured_at": datetime(2024, 1, 2)},
        ]

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = fake_rows

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        poller._conn = mock_conn

        events = poller._fetch_new_events()
        assert isinstance(events, list)
        assert len(events) == 2
        assert events[0]["operation"] == "INSERT"

    def test_fetch_returns_only_events_above_watermark(self):
        """
        _fetch_new_events uses WHERE event_id > watermark.
        The SQL must be called with the current watermark value.
        """
        from producer.db_poller import CDCPoller
        poller = CDCPoller()
        poller._last_event_id = 5   # simulate a resumed session

        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        poller._conn = mock_conn

        poller._fetch_new_events()

        # Confirm the SQL was called with watermark=5
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1] == (5,), "SQL must filter by current watermark"

    def test_publish_event_calls_send(self):
        """_publish_event should call producer.send with the event."""
        from producer.db_poller import CDCPoller
        poller = CDCPoller()

        mock_future = MagicMock()
        mock_producer = MagicMock()
        mock_producer.send.return_value = mock_future
        poller._producer = mock_producer

        event = {"event_id": 1, "operation": "INSERT", "customer_id": 5}
        poller._publish_event(event)
        mock_producer.send.assert_called_once()


# ══════════════════════════════════════════════════════════════════
# 4.  Consumer – ADLSUploader
# ══════════════════════════════════════════════════════════════════

class TestADLSUploader:

    @patch("consumer.adls_uploader.DataLakeServiceClient")
    def test_connect_creates_client(self, mock_adls_cls):
        """connect() should instantiate the DataLakeServiceClient."""
        from consumer.adls_uploader import ADLSUploader
        uploader = ADLSUploader()
        uploader.connect()
        mock_adls_cls.assert_called_once()

    @patch("consumer.adls_uploader.DataLakeServiceClient")
    def test_upload_event_success(self, mock_adls_cls):
        """upload_event should call upload_data exactly once."""
        from consumer.adls_uploader import ADLSUploader

        mock_file_client = MagicMock()
        mock_fs_client = MagicMock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_service = MagicMock()
        mock_service.get_file_system_client.return_value = mock_fs_client
        mock_adls_cls.return_value = mock_service

        uploader = ADLSUploader()
        uploader.connect()

        event = {"event_id": 99, "operation": "DELETE", "customer_id": 3}
        path = uploader.upload_event(event)

        mock_file_client.upload_data.assert_called_once()
        assert "cdc_" in path
        assert str(event["event_id"]) in path

    @patch("consumer.adls_uploader.DataLakeServiceClient")
    def test_upload_uses_overwrite_true(self, mock_adls_cls):
        """
        upload_data must be called with overwrite=True.
        This is the idempotency guard — re-uploading the same event_id
        replaces the file instead of creating a duplicate.
        """
        from consumer.adls_uploader import ADLSUploader

        mock_file_client = MagicMock()
        mock_fs_client = MagicMock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_service = MagicMock()
        mock_service.get_file_system_client.return_value = mock_fs_client
        mock_adls_cls.return_value = mock_service

        uploader = ADLSUploader()
        uploader.connect()
        uploader.upload_event({"event_id": 1, "operation": "INSERT"})

        _, kwargs = mock_file_client.upload_data.call_args
        assert kwargs.get("overwrite") is True, "overwrite=True is required for idempotency"

    def test_upload_without_connect_raises(self):
        """Calling upload_event before connect should raise RuntimeError."""
        from consumer.adls_uploader import ADLSUploader
        uploader = ADLSUploader()
        with pytest.raises(RuntimeError, match="connect\\(\\)"):
            uploader.upload_event({"event_id": 1})


# ══════════════════════════════════════════════════════════════════
# 5.  JSON serialiser helper
# ══════════════════════════════════════════════════════════════════

def test_json_serialiser_handles_datetime():
    """_json_serialiser should convert datetime to ISO string."""
    from producer.db_poller import _json_serialiser
    dt = datetime(2024, 6, 15, 12, 0, 0)
    result = _json_serialiser(dt)
    assert result == "2024-06-15T12:00:00"


def test_json_serialiser_raises_for_unknown_type():
    """_json_serialiser should raise TypeError for unsupported types."""
    from producer.db_poller import _json_serialiser
    with pytest.raises(TypeError):
        _json_serialiser({"set", "is", "not", "serialisable"})


# ══════════════════════════════════════════════════════════════════
# 6.  End-to-end message flow (integration-style, fully mocked)
# ══════════════════════════════════════════════════════════════════

@patch("consumer.adls_uploader.DataLakeServiceClient")
@patch("consumer.kafka_consumer.KafkaConsumer")
def test_consumer_processes_message(mock_kafka_cls, mock_adls_cls):
    """
    Simulate one Kafka message flowing through CDCConsumer into ADLS.
    The loop terminates because the mock consumer is an iterable with one item.
    """
    from consumer.kafka_consumer import CDCConsumer

    event = {
        "event_id": 42, "operation": "UPDATE",
        "customer_id": 7, "customer_name": "Praveen",
        "email": "praveen@example.com",
    }
    fake_message = MagicMock()
    fake_message.value = event
    fake_message.partition = 0
    fake_message.offset = 100

    # Create a mock consumer that is iterable and has a close() method
    mock_consumer = MagicMock()
    mock_consumer.__iter__.return_value = iter([fake_message])
    mock_kafka_cls.return_value = mock_consumer

    mock_file_client = MagicMock()
    mock_fs_client = MagicMock()
    mock_fs_client.get_file_client.return_value = mock_file_client
    mock_service = MagicMock()
    mock_service.get_file_system_client.return_value = mock_fs_client
    mock_adls_cls.return_value = mock_service

    consumer = CDCConsumer()
    consumer.run()

    mock_file_client.upload_data.assert_called_once()
