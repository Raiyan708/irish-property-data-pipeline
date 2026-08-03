"""PySpark batch job: Delta Silver -> Delta Gold.

Reads the full Silver property_transactions table, aggregates it into a
county/year/property-type price summary, and overwrites the Gold Delta
table. Unlike Bronze/Silver, this is a plain batch job (not Structured
Streaming) -- aggregates are cheap to recompute from scratch at this data
size, and doing so avoids the complexity of incremental streaming
aggregation.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, median, min as spark_min, year

from streaming import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ppr.streaming.gold_transform")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ppr-gold-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_silver(spark: SparkSession) -> DataFrame:
    """Read the full Silver Delta table as a plain (non-streaming) batch."""
    return spark.read.format("delta").load(config.SILVER_PATH)


def compute_gold_summary(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.withColumn("year", year(col("date_of_sale")))
        .groupBy("county", "year", "description_of_property")
        .agg(
            count("*").alias("transaction_count"),
            avg("price_eur").alias("avg_price_eur"),
            median("price_eur").alias("median_price_eur"),
            spark_min("price_eur").alias("min_price_eur"),
            spark_max("price_eur").alias("max_price_eur"),
        )
    )


def run() -> None:
    logger.info("Starting PPR gold transform job")
    logger.info("Silver table path: %s", config.SILVER_PATH)
    logger.info("Gold table path: %s", config.GOLD_PATH)

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver_df = read_silver(spark)
        gold_df = compute_gold_summary(silver_df)
        gold_df.write.format("delta").mode("overwrite").save(config.GOLD_PATH)
    except Exception:
        logger.exception("Gold transform job failed")
        raise
    finally:
        spark.stop()

    logger.info("Gold transform job finished")


if __name__ == "__main__":
    run()
