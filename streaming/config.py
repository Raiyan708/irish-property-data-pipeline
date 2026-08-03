"""Configuration for the PySpark Structured Streaming bronze ingestion job."""

import os

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "property_transactions")
KAFKA_STARTING_OFFSETS: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

BRONZE_PATH: str = os.getenv("BRONZE_PATH", "data/delta/bronze/property_transactions")
CHECKPOINT_PATH: str = os.getenv(
    "BRONZE_CHECKPOINT_PATH", "data/delta/_checkpoints/bronze/property_transactions"
)

SILVER_PATH: str = os.getenv("SILVER_PATH", "data/delta/silver/property_transactions")
SILVER_CHECKPOINT_PATH: str = os.getenv(
    "SILVER_CHECKPOINT_PATH", "data/delta/_checkpoints/silver/property_transactions"
)

# "once" runs the query until the topic is drained then stops (good for batch-style
# backfills and testing); "continuous" keeps the stream running indefinitely.
TRIGGER_MODE: str = os.getenv("STREAMING_TRIGGER_MODE", "once")
PROCESSING_TIME_INTERVAL: str = os.getenv("STREAMING_PROCESSING_TIME", "30 seconds")
