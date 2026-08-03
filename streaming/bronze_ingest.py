"""PySpark Structured Streaming job: Kafka -> Delta Lake Bronze.

Consumes raw PPR transaction messages from the `property_transactions` Kafka
topic and appends them, largely unmodified, into a Delta Lake Bronze table.
Bronze keeps every source column as a string (see streaming/schemas.py) and
adds ingestion metadata so downstream Silver transforms have full lineage
back to the originating Kafka record.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json

from streaming import config
from streaming.schemas import COLUMN_NAME_MAP, PPR_RAW_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ppr.streaming.bronze_ingest")


def build_spark_session() -> SparkSession:
    """Create a SparkSession configured with Delta Lake support."""
    return (
        SparkSession.builder.appName("ppr-bronze-ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Subscribe to the Kafka topic and return the raw streaming DataFrame."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .load()
    )


def parse_bronze_records(raw_stream: DataFrame) -> DataFrame:
    """Parse Kafka JSON payloads into the raw PPR schema plus ingestion metadata."""
    parsed = raw_stream.select(
        from_json(col("value").cast("string"), PPR_RAW_SCHEMA).alias("data"),
        col("key").cast("string").alias("_kafka_key"),
        col("topic").alias("_kafka_topic"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_timestamp"),
    )
    flattened = parsed.select(
        "data.*", "_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp"
    )

    for raw_name, bronze_name in COLUMN_NAME_MAP.items():
        flattened = flattened.withColumnRenamed(raw_name, bronze_name)

    return flattened.withColumn("_ingested_at", current_timestamp())


def run() -> None:
    logger.info("Starting PPR bronze ingestion job")
    logger.info("Kafka bootstrap servers: %s", config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Kafka topic: %s", config.KAFKA_TOPIC)
    logger.info("Bronze table path: %s", config.BRONZE_PATH)
    logger.info("Checkpoint path: %s", config.CHECKPOINT_PATH)
    logger.info("Trigger mode: %s", config.TRIGGER_MODE)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_stream = read_kafka_stream(spark)
        bronze_records = parse_bronze_records(raw_stream)

        writer = bronze_records.writeStream.format("delta").outputMode("append").option(
            "checkpointLocation", config.CHECKPOINT_PATH
        )

        if config.TRIGGER_MODE == "once":
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime=config.PROCESSING_TIME_INTERVAL)

        query = writer.start(config.BRONZE_PATH)
        query.awaitTermination()
    except Exception:
        logger.exception("Bronze ingestion job failed")
        raise
    finally:
        spark.stop()

    logger.info("Bronze ingestion job finished")


if __name__ == "__main__":
    run()
