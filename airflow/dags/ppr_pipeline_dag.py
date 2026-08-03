"""Irish Property Price Register pipeline: producer -> Bronze -> Silver -> Gold -> BigQuery -> dbt.

Runs the Docker containers we've been triggering by hand (producer,
spark-bronze, spark-silver, spark-gold, load-gold, dbt run, dbt test)
in order, once a day.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = "/Users/muhammadraiyan/irish-property-data-pipeline"

with DAG(
    dag_id="ppr_pipeline",
    description="Irish Property Price Register: producer -> Bronze -> Silver -> Gold -> BigQuery -> dbt",
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    run_producer = DockerOperator(
        task_id="run_producer",
        image="docker-producer:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/ingestion", target="/app/ingestion", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
        ],
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "KAFKA_TOPIC": "property_transactions",
            "CSV_PATH": "/app/data/raw/ppr_all.csv",
            "PYTHONPATH": "/app",
        },
        command="python -m ingestion.producer",
    )

    run_bronze = DockerOperator(
        task_id="run_bronze",
        image="docker-spark-bronze:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/streaming", target="/app/streaming", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
        ],
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "KAFKA_TOPIC": "property_transactions",
            "BRONZE_PATH": "/app/data/delta/bronze/property_transactions",
            "BRONZE_CHECKPOINT_PATH": "/app/data/delta/_checkpoints/bronze/property_transactions",
            "STREAMING_TRIGGER_MODE": "once",
            "PYTHONPATH": "/app",
        },
        command=(
            "/opt/spark/bin/spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "streaming/bronze_ingest.py"
        ),
    )
    run_silver = DockerOperator(
        task_id="run_silver",
        image="docker-spark-silver:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/streaming", target="/app/streaming", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
        ],
        environment={
            "BRONZE_PATH": "/app/data/delta/bronze/property_transactions",
            "SILVER_PATH": "/app/data/delta/silver/property_transactions",
            "SILVER_CHECKPOINT_PATH": "/app/data/delta/_checkpoints/silver/property_transactions",
            "PYTHONPATH": "/app",
        },
        command=(
            "/opt/spark/bin/spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "streaming/silver_transform.py"
        ),
    )
    run_gold = DockerOperator(
        task_id="run_gold",
        image="docker-spark-gold:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/streaming", target="/app/streaming", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
        ],
        environment={
            "SILVER_PATH": "/app/data/delta/silver/property_transactions",
            "GOLD_PATH": "/app/data/delta/gold/county_yearly_price_summary",
            "PYTHONPATH": "/app",
        },
        command=(
            "/opt/spark/bin/spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "streaming/gold_transform.py"
        ),
    )

    run_load_gold = DockerOperator(
        task_id="run_load_gold",
        image="docker-load-gold:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/warehouse", target="/app/warehouse", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/data", target="/app/data", type="bind"),
            Mount(
                source=f"{PROJECT_ROOT}/gcp-credentials.json",
                target="/app/gcp-credentials.json",
                type="bind",
                read_only=True,
            ),
        ],
        environment={
            "GOLD_PATH": "/app/data/delta/gold/county_yearly_price_summary",
            "GCP_PROJECT_ID": "irish-ppr-pipeline",
            "BQ_DATASET": "ppr_warehouse",
            "BQ_TABLE": "county_yearly_price_summary",
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYTHONPATH": "/app",
        },
        command="python -m warehouse.load_gold",
    )

    dbt_mounts = [
        Mount(source=f"{PROJECT_ROOT}/dbt", target="/app/dbt", type="bind"),
        Mount(
            source=f"{PROJECT_ROOT}/gcp-credentials.json",
            target="/app/gcp-credentials.json",
            type="bind",
            read_only=True,
        ),
    ]
    dbt_environment = {
        "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
        "DBT_PROFILES_DIR": "/app/dbt",
    }

    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="docker-dbt:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app/dbt",
        mounts=dbt_mounts,
        environment=dbt_environment,
        entrypoint="dbt",
        command="run",
    )
    test_dbt = DockerOperator(
        task_id="test_dbt",
        image="docker-dbt:latest",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="docker_default",
        working_dir="/app/dbt",
        mounts=dbt_mounts,
        environment=dbt_environment,
        entrypoint="dbt",
        command="test",
    )

    run_producer >> run_bronze >> run_silver >> run_gold >> run_load_gold >> run_dbt >> test_dbt
