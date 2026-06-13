"""
reset_watermark.py
──────────────────
Utility to reset the producer watermark back to 0.

USE ONLY when you want the producer to replay ALL existing
cdc_events rows from the beginning (e.g. during development
or after wiping ADLS Bronze storage).

WARNING: Running this while the producer is active will cause
         all historic CDC events to be re-sent to Kafka and
         re-uploaded to ADLS. Stop the producer first.

Usage:
    python reset_watermark.py
"""

import json
from pathlib import Path

WATERMARK_FILE = Path(__file__).resolve().parent / "watermark.json"


def reset():
    if WATERMARK_FILE.exists():
        old = json.loads(WATERMARK_FILE.read_text())
        print(f"Current watermark: last_event_id={old.get('last_event_id', 0)}")
        confirm = input("Reset to 0? This will re-process ALL cdc_events. [y/N]: ")
        if confirm.strip().lower() != "y":
            print("Aborted.")
            return
    WATERMARK_FILE.write_text(json.dumps({"last_event_id": 0}, indent=2))
    print("Watermark reset to 0.")


if __name__ == "__main__":
    reset()
