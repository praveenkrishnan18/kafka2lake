"""
config/settings.py
──────────────────
Loads all environment variables from .env and exposes them
as a single Settings object imported by other modules.
"""

import os
from dotenv import load_dotenv

# Load .env from project root (one level up from config/)
load_dotenv()


class Settings:
    # ── PostgreSQL ────────────────────────────────────────────
    POSTGRES_HOST: str     = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int     = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_USER: str     = os.getenv("POSTGRES_USER", "cdc_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "cdc_password")
    POSTGRES_DB: str       = os.getenv("POSTGRES_DB", "cdc_db")

    # ── Kafka ─────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str             = os.getenv("KAFKA_TOPIC", "cdc_customers")
    KAFKA_GROUP_ID: str          = os.getenv("KAFKA_GROUP_ID", "cdc_consumer_group")

    # ── Azure ADLS Gen2 ───────────────────────────────────────
    ADLS_ACCOUNT_NAME: str  = os.getenv("ADLS_ACCOUNT_NAME", "")
    ADLS_ACCOUNT_KEY: str   = os.getenv("ADLS_ACCOUNT_KEY", "")
    ADLS_CONTAINER_NAME: str = os.getenv("ADLS_CONTAINER_NAME", "bronze")
    ADLS_BRONZE_PATH: str   = os.getenv("ADLS_BRONZE_PATH", "cdc/customers/")

    # ── App ───────────────────────────────────────────────────
    POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
    LOG_LEVEL: str             = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
