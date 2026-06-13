"""
run_consumer.py
────────────────
Entry point to start the CDC consumer (Kafka → ADLS Gen2).
Run from the project root:

    python run_consumer.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from consumer.kafka_consumer import CDCConsumer

if __name__ == "__main__":
    CDCConsumer().run()
