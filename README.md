# Irish Property Price Register — Data Pipeline

**Live dashboard**: https://irish-property-prices.streamlit.app

A local, Docker-based data pipeline that ingests the real [Property Price
Register](https://www.propertypriceregister.ie/) CSV (797,775 real Irish
property sales), streams it through Kafka into Delta Lake (Bronze → Silver →
Gold), loads it into BigQuery, polishes it with dbt, orchestrates all of it
with Airflow, and presents it in a Streamlit dashboard.


## Project structure

```
data/raw/ppr_all.csv     # the real source data (110MB, not in git)
data/delta/               # Bronze/Silver/Gold Delta tables (generated, not in git)
gcp-credentials.json      # BigQuery service account key (not in git)

ingestion/producer.py     # Step 1: reads the CSV, publishes each row to Kafka
streaming/
  bronze_ingest.py        # Step 2: Kafka -> Delta Bronze (raw copy)
  silver_transform.py     # Step 3: Bronze -> Delta Silver (cleaned, deduplicated)
  gold_transform.py       # Step 4: Silver -> Delta Gold (aggregated by county/year/type)
warehouse/load_gold.py    # Step 5: Delta Gold -> BigQuery
dbt/models/                # Step 6: SQL models that polish the BigQuery table
airflow/dags/ppr_pipeline_dag.py  # orchestrates steps 1-6, once a day
dashboard/app.py           # Step 7: the Streamlit dashboard

docker/docker-compose.yml # defines every container above
```

## Running it

Requires Docker Desktop running.

### Cold start (from nothing)

```bash
# 1. Always-on infrastructure
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-ui postgres

# 2. First time only (or after wiping the Postgres volume): init Airflow's DB
docker compose -f docker/docker-compose.yml run --rm airflow-init

# 3. Airflow + dashboard
docker compose -f docker/docker-compose.yml up -d airflow-webserver airflow-scheduler dashboard
```

Then open **Airflow** at http://localhost:8081 (login `admin`/`admin`), unpause
the `ppr_pipeline` DAG, and trigger it. Watch the Graph view — seven tasks run
in order (`run_producer` → `run_bronze` → `run_silver` → `run_gold` →
`run_load_gold` → `run_dbt` → `test_dbt`), taking about 2 minutes total.

Once it finishes, open the **dashboard** at http://localhost:8501.

### Running one step manually (for debugging)

```bash
docker compose -f docker/docker-compose.yml run --rm producer
docker compose -f docker/docker-compose.yml run --rm spark-bronze
docker compose -f docker/docker-compose.yml run --rm spark-silver
docker compose -f docker/docker-compose.yml run --rm spark-gold
docker compose -f docker/docker-compose.yml run --rm load-gold
docker compose -f docker/docker-compose.yml run --rm dbt run
docker compose -f docker/docker-compose.yml run --rm dbt test
```

### Other UIs

- **Kafka UI**: http://localhost:8080 — browse the `property_transactions` topic
- **BigQuery console**: project `irish-ppr-pipeline`, dataset `ppr_warehouse` —
  tables `county_yearly_price_summary` (raw Gold) and `property_price_summary`
  (dbt's polished version, with year-over-year price change)

### Shutting down

```bash
docker compose -f docker/docker-compose.yml down
```

This stops everything but preserves all data (Kafka topic, Delta tables,
Airflow's Postgres DB) for next time.

## Demo walkthrough

1. Show `data/raw/ppr_all.csv` — a real government export, 797,775 rows.
2. Open Kafka UI — the queue the data flows through first.
3. Trigger the Airflow DAG and watch the Graph view turn green step by step.
4. Open the BigQuery console and show the two tables.
5. Open the Streamlit dashboard — filter by county, point out the map, and
   that the trend chart shows the real 2008 Irish property crash and recovery.

## Prerequisites

- Docker Desktop
- A GCP project with BigQuery enabled and a service account key saved as
  `gcp-credentials.json` in the project root (see `.env.example` for other
  configurable env vars)
