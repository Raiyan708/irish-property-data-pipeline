"""PySpark Structured Streaming job: Delta Bronze -> Delta Silver.

Reads the Bronze property_transactions table as a stream, cleans/types it,
and upserts it into Silver -- deduplicating against rows already written,
since Bronze is append-only and can contain replayed/duplicate messages.
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, trim

from streaming import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ppr.streaming.silver_transform")

# Real PPR data has no unique transaction ID, so this is our best stand-in
# for "the same sale": same address, same date, same price.
DEDUP_KEY_COLUMNS = ["address", "date_of_sale", "price_eur"]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ppr-silver-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_bronze_stream(spark: SparkSession) -> DataFrame:
    """Read the Bronze Delta table as a stream (not a one-off batch read)."""
    return spark.readStream.format("delta").load(config.BRONZE_PATH)


def transform_to_silver(bronze_stream: DataFrame) -> DataFrame:
    return bronze_stream.withColumn(
        "date_of_sale", to_date(col("date_of_sale"), "dd/MM/yyyy")
    ).withColumn(
        "price_eur", regexp_replace(col("price_eur"), "[^0-9.]", "").cast("decimal(12,2)")
    ).withColumn(
        "not_full_market_price", col("not_full_market_price") == "Yes"
    ).withColumn(
        "vat_exclusive", col("vat_exclusive") == "Yes"
    ).withColumn(
        "address", trim(regexp_replace(col("address"), " +", " "))
    ).withColumn(
        "county", trim(regexp_replace(col("county"), " +", " "))
    ).withColumn(
        "description_of_property", trim(regexp_replace(col("description_of_property"), " +", " "))
    ).withColumn(
        "property_size_description", trim(regexp_replace(col("property_size_description"), " +", " "))
    )


def upsert_to_silver(batch_df: DataFrame, batch_id: int) -> None:
    """Write one micro-batch into Silver, skipping rows already present.

    Called once per micro-batch by writeStream.foreachBatch. batch_df is a
    plain, bounded DataFrame here (not a stream), so operations like
    dropDuplicates and DeltaTable.merge -- which aren't allowed on an
    unbounded streaming DataFrame -- are safe to use.
    """
    batch_df = batch_df.dropDuplicates(DEDUP_KEY_COLUMNS)
    spark = batch_df.sparkSession

    if not DeltaTable.isDeltaTable(spark, config.SILVER_PATH):
        logger.info("Silver table doesn't exist yet -- creating it with this batch")
        batch_df.write.format("delta").save(config.SILVER_PATH)
        return

    silver_table = DeltaTable.forPath(spark, config.SILVER_PATH)
    merge_condition = " AND ".join(f"silver.{c} = new.{c}" for c in DEDUP_KEY_COLUMNS)

    (
        silver_table.alias("silver")
        .merge(batch_df.alias("new"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def run() -> None:
    logger.info("Starting PPR silver transform job")
    logger.info("Bronze table path: %s", config.BRONZE_PATH)
    logger.info("Silver table path: %s", config.SILVER_PATH)
    logger.info("Checkpoint path: %s", config.SILVER_CHECKPOINT_PATH)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_stream = read_bronze_stream(spark)
        silver_records = transform_to_silver(bronze_stream)

        query = (
            silver_records.writeStream.foreachBatch(upsert_to_silver)
            .option("checkpointLocation", config.SILVER_CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
    except Exception:
        logger.exception("Silver transform job failed")
        raise
    finally:
        spark.stop()

    logger.info("Silver transform job finished")


if __name__ == "__main__":
    run()
