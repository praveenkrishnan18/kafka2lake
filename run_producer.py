"""
run_producer.py
───────────────
Entry point to start the CDC producer (DB poller → Kafka).
Run from the project root:

    python run_producer.py
"""

import sys
import os

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.dirname(__file__))

from producer.db_poller import CDCPoller

if __name__ == "__main__":
    CDCPoller().run()
