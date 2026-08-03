"""Configuration for loading Gold Delta data into BigQuery."""

import os

GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "irish-ppr-pipeline")
BQ_DATASET: str = os.getenv("BQ_DATASET", "ppr_warehouse")
BQ_TABLE: str = os.getenv("BQ_TABLE", "county_yearly_price_summary")

GOLD_PATH: str = os.getenv("GOLD_PATH", "data/delta/gold/county_yearly_price_summary")
