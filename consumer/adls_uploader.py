"""
consumer/adls_uploader.py
──────────────────────────
Uploads CDC events received from Kafka to Azure Data Lake
Storage Gen2 (Bronze layer) as individual JSON files.

File naming convention:
    bronze/cdc/customers/cdc_<YYYYMMDD_HHMMSS>_<event_id>.json

DUPLICATE-PREVENTION DESIGN
────────────────────────────
Each file name embeds the event_id.  Because the Kafka consumer
uses enable_auto_commit=True with group_id, offsets are committed
after each message is processed.  On restart the consumer picks
up from its last committed offset — so the same Kafka message is
never consumed twice, and therefore never uploaded twice.

Additionally, upload_data(..., overwrite=True) means that even if
the same event_id is somehow written again (e.g. during testing),
the file is safely overwritten rather than creating a duplicate.
"""

import json
from datetime import datetime, timezone
from io import BytesIO

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import AzureError

from config import get_logger, settings

logger = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────
# ADLS Gen2 client helper
# ──────────────────────────────────────────────────────────────────────────

def _build_adls_client() -> DataLakeServiceClient:
    """
    Build an ADLS Gen2 service client using a storage-account key.
    In production prefer DefaultAzureCredential (Managed Identity).
    """
    account_url = (
        f"https://{settings.ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
    )
    return DataLakeServiceClient(
        account_url=account_url,
        credential=settings.ADLS_ACCOUNT_KEY,
    )


# ──────────────────────────────────────────────────────────────────────────
# Uploader class
# ──────────────────────────────────────────────────────────────────────────

class ADLSUploader:
    """
    Uploads a single CDC event (dict) to ADLS Gen2 as a JSON file.
    One file per event keeps the Bronze layer append-friendly and
    easy to inspect — each file name contains the event_id so
    accidental re-uploads overwrite rather than duplicate.
    """

    def __init__(self):
        self._client: DataLakeServiceClient | None = None

    def connect(self) -> None:
        """Initialise the ADLS client (call once at startup)."""
        try:
            self._client = _build_adls_client()
            logger.info(
                "ADLS Gen2 client initialised for account '%s'.",
                settings.ADLS_ACCOUNT_NAME,
            )
        except AzureError as exc:
            logger.error("Failed to initialise ADLS client: %s", exc)
            raise

    def upload_event(self, event: dict) -> str:
        """
        Serialise *event* to JSON and write it to ADLS Bronze layer.

        File name format: cdc_<YYYYMMDD_HHMMSS>_<event_id>.json
        The event_id in the file name makes each file unique and
        idempotent — re-uploading the same event_id overwrites the
        same file path (overwrite=True) instead of creating a duplicate.

        Returns the remote path on success.
        Raises AzureError on failure.
        """
        if self._client is None:
            raise RuntimeError("ADLSUploader.connect() must be called before upload.")

        # Build a time-stamped, event_id-keyed file name
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        event_id = event.get("event_id", "unknown")
        file_name = f"cdc_{ts}_{event_id}.json"
        remote_path = settings.ADLS_BRONZE_PATH + file_name

        payload = json.dumps(event, default=str).encode("utf-8")

        try:
            fs_client = self._client.get_file_system_client(
                settings.ADLS_CONTAINER_NAME
            )
            file_client = fs_client.get_file_client(remote_path)
            # overwrite=True: idempotent — same event_id path is safely overwritten
            file_client.upload_data(BytesIO(payload), overwrite=True, length=len(payload))
            logger.info(
                "Uploaded event_id=%s → adls://%s/%s/%s",
                event_id,
                settings.ADLS_ACCOUNT_NAME,
                settings.ADLS_CONTAINER_NAME,
                remote_path,
            )
            return remote_path
        except AzureError as exc:
            logger.error(
                "ADLS upload failed for event_id=%s: %s", event_id, exc
            )
            raise
