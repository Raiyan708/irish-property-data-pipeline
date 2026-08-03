"""Loads the Gold Delta table into BigQuery.

Reads the Gold Delta table (county_yearly_price_summary) directly via the
deltalake package -- no Spark/JVM needed, since Gold is small enough to
move as a single in-memory pandas DataFrame. Every run fully replaces the
BigQuery table's contents (WRITE_TRUNCATE), mirroring how Gold itself is
recomputed from scratch each time (see streaming/gold_transform.py).

Authentication is picked up automatically by the BigQuery client from the
GOOGLE_APPLICATION_CREDENTIALS environment variable.
"""

import logging
import sys

import pandas as pd
from deltalake import DeltaTable
from google.cloud import bigquery

from warehouse import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("ppr.warehouse.load_gold")


def read_gold() -> pd.DataFrame:
    """Read the Gold Delta table into a pandas DataFrame."""
    delta_table = DeltaTable(config.GOLD_PATH)
    return delta_table.to_pandas()


def load_to_bigquery(df: pd.DataFrame, table_id: str) -> None:
    """Overwrite the BigQuery table with the given DataFrame's contents."""
    client = bigquery.Client(project=config.GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def run() -> None:
    logger.info("Starting Gold -> BigQuery load")
    logger.info("Gold Delta path: %s", config.GOLD_PATH)
    table_id = f"{config.GCP_PROJECT_ID}.{config.BQ_DATASET}.{config.BQ_TABLE}"
    logger.info("BigQuery table: %s", table_id)

    try:
        gold_df = read_gold()
        logger.info("Read %d rows from Gold", len(gold_df))
        load_to_bigquery(gold_df, table_id)
    except Exception:
        logger.exception("Gold -> BigQuery load failed")
        sys.exit(1)

    logger.info("Load finished: %d rows loaded into %s", len(gold_df), table_id)


if __name__ == "__main__":
    run()
