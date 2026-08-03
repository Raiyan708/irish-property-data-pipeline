"""BigQuery data access for the Streamlit dashboard."""

import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "irish-ppr-pipeline")
BQ_DATASET = os.getenv("BQ_DATASET", "ppr_warehouse")
BQ_TABLE = os.getenv("BQ_TABLE", "property_price_summary")


@st.cache_data(ttl=3600)
def load_price_summary() -> pd.DataFrame:
    """Load the full dbt-built property_price_summary table from BigQuery."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    query = f"SELECT * FROM `{table_id}` ORDER BY county, property_type, year"
    return client.query(query).result().to_dataframe()
