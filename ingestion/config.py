"""Configuration for the Kafka ingestion layer, sourced from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "property_transactions")

CSV_PATH: Path = Path(os.getenv("CSV_PATH", PROJECT_ROOT / "data" / "raw" / "ppr_all.csv"))

PRODUCER_ACKS: str = os.getenv("KAFKA_PRODUCER_ACKS", "all")
PRODUCER_RETRIES: int = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
LOG_EVERY_N_ROWS: int = int(os.getenv("LOG_EVERY_N_ROWS", "1000"))
