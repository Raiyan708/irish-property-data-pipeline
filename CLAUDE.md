# Irish Property Price Register — Data Pipeline

## Architecture
- Ingestion: PPR CSV → Apache Kafka
- Stream Processing: PySpark Structured Streaming
- Storage: Delta Lake (Bronze / Silver / Gold)
- Orchestration: Apache Airflow
- Transformation: dbt Core
- Warehouse: Google BigQuery (europe-west1)
- Dashboard: Streamlit

## Tech Stack
Python 3.11, Apache Kafka, PySpark, Delta Lake,
Apache Airflow, dbt Core, BigQuery, Docker, Git

## Environment
- GCP Project ID: irish-ppr-pipeline
- BigQuery Dataset: ppr_warehouse
- Credentials file: gcp-credentials.json (never commit)

## Coding Standards
- Python files use type hints
- dbt models have schema.yml tests and descriptions
- Airflow DAGs have docstrings
- Git commits follow: feat: fix: docs: refactor:

## Project Structure
├── ingestion/
├── streaming/
├── dbt/
├── airflow/
├── dashboard/
├── docker/
├── data/raw/
├── tests/
└── docs/