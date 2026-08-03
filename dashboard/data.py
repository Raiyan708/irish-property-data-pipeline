"""BigQuery data access for the Streamlit dashboard."""

import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "irish-ppr-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "ppr_warehouse")
BQ_TABLE = os.getenv("BQ_TABLE", "property_price_summary")


PRICE_COLUMNS = ["avg_price_eur", "median_price_eur", "min_price_eur", "max_price_eur"]
INT_COLUMNS = ["year", "transaction_count"]


def _get_bigquery_client() -> bigquery.Client:
    """Build a BigQuery client.

    Prefers credentials from Streamlit secrets (used on Streamlit Community
    Cloud, which has no file system to mount a credentials file into).
    Falls back to GOOGLE_APPLICATION_CREDENTIALS, used for local/Docker runs
    where no secrets.toml exists at all.
    """
    try:
        service_account_info = dict(st.secrets["gcp_service_account"])
    except Exception:
        return bigquery.Client(project=GCP_PROJECT_ID)

    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)


@st.cache_data(ttl=3600)
def load_price_summary() -> pd.DataFrame:
    """Load the full dbt-built property_price_summary table from BigQuery."""
    client = _get_bigquery_client()
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
