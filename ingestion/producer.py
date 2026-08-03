"""Kafka producer that streams Irish Property Price Register (PPR) transactions.

Reads data/raw/ppr_all.csv row by row and publishes each row as a JSON
message to the `property_transactions` Kafka topic.
"""

import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any, Iterator

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ppr.ingestion.producer")


def build_producer() -> KafkaProducer:
    """Create a KafkaProducer connected to the configured bootstrap servers."""
    try:
        return KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=config.PRODUCER_ACKS,
            retries=config.PRODUCER_RETRIES,
            linger_ms=50,
        )
    except NoBrokersAvailable:
        logger.error(
            "Could not connect to Kafka brokers at %s. Is Docker Compose running?",
            config.KAFKA_BOOTSTRAP_SERVERS,
        )
        raise


def read_transactions(csv_path: Path) -> Iterator[dict[str, Any]]:
    """Yield each row of the PPR CSV as a dict, keyed by column header."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with csv_path.open("r", encoding="cp1252", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def publish_transactions(producer: KafkaProducer, csv_path: Path, topic: str) -> tuple[int, int]:
    """Publish every row in csv_path to the given Kafka topic.

    Sends are asynchronous -- each row is queued and the loop moves on
    immediately, letting kafka-python batch multiple rows per network
    request instead of waiting for each row's ack before sending the next.
    Delivery is confirmed via callbacks, and producer.flush() at the end
    blocks until every queued message has actually been acknowledged.

    Returns a (sent_count, failed_count) tuple.
    """
    counts = {"sent": 0, "failed": 0}

    def on_success(_metadata: object) -> None:
        counts["sent"] += 1

    def on_error(row_number: int, row: dict[str, Any], exc: Exception) -> None:
        counts["failed"] += 1
        logger.error("Failed to publish row %d: %s (%s)", row_number, row, exc)

    for row_number, row in enumerate(read_transactions(csv_path), start=1):
        message_key = f"{row.get('Address', '')}-{row.get('Date of Sale (dd/mm/yyyy)', row_number)}"

        try:
            producer.send(topic, key=message_key, value=row).add_callback(
                on_success
            ).add_errback(lambda exc, rn=row_number, r=row: on_error(rn, r, exc))
        except KafkaError:
            counts["failed"] += 1
            logger.exception("Failed to queue row %d: %s", row_number, row)
            continue

        if row_number % config.LOG_EVERY_N_ROWS == 0:
            logger.info("Queued %d rows so far...", row_number)

    producer.flush()
    return counts["sent"], counts["failed"]


def main() -> None:
    logger.info("Starting PPR Kafka producer")
    logger.info("Bootstrap servers: %s", config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", config.KAFKA_TOPIC)
    logger.info("Source CSV: %s", config.CSV_PATH)

    try:
        producer = build_producer()
    except NoBrokersAvailable:
        sys.exit(1)

    try:
        sent_count, failed_count = publish_transactions(producer, config.CSV_PATH, config.KAFKA_TOPIC)
    except FileNotFoundError as exc:
        logger.error(str(exc))
        sys.exit(1)
    finally:
        producer.flush()
        producer.close()

    logger.info("Done. Sent: %d, Failed: %d", sent_count, failed_count)

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
