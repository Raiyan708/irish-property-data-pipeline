"""BigQuery data access for the Streamlit dashboard."""

import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "irish-ppr-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "ppr_warehouse")
BQ_TABLE = os.getenv("BQ_TABLE", "property_price_summary")


PRICE_COLUMNS = ["avg_price_eur", "median_price_eur", "min_price_eur", "max_price_eur"]
INT_COLUMNS = ["year", "transaction_count"]


@st.cache_data(ttl=3600)
def load_price_summary() -> pd.DataFrame:
    """Load the full dbt-built property_price_summary table from BigQuery."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"SELECT * FROM `{table_id}` ORDER BY county, property_type, year"
    df = client.query(query).result().to_dataframe()

    # BigQuery NUMERIC columns come back as Python Decimal objects (object
    # dtype), and INTEGER columns come back as pandas' *nullable* Int64
    # extension type -- both are known to crash pyarrow's dataframe-to-arrow
    # conversion (which Streamlit uses for st.dataframe) in some version
    # combinations. Cast to plain numpy dtypes to avoid that entirely.
    for col in PRICE_COLUMNS:
        df[col] = df[col].astype(float)
    for col in INT_COLUMNS:
        df[col] = df[col].astype("int64")

    return df
