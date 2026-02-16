"""
Airflow DAG for COVID-19 ETL Pipeline
Orchestrates daily execution of Extract, Transform, Load processes
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import COVID19Pipeline # noqa


def extract_data(**context):
    """Task: Extract data from sources."""
    pipeline = COVID19Pipeline()
    extracted_data = pipeline.run_extract()

    # Push to XCom
    context["task_instance"].xcom_push(key="extracted_data", value=extracted_data)

    return f"Extracted {len(extracted_data)} datasets"


def transform_data(**context):
    """Task: Transform data with PySpark."""
    pipeline = COVID19Pipeline()

    # Pull from XCom
    extracted_data = context["task_instance"].xcom_pull(
        task_ids="extract_data", key="extracted_data"
    )

    transformed_data = pipeline.run_transform(extracted_data)

    # Push to XCom
    context["task_instance"].xcom_push(key="transformed_data", value=transformed_data)

    # Cleanup Spark
    pipeline.transformer.stop_spark()

    return f"Transformed {len(transformed_data)} datasets"


def load_data(**context):
    """Task: Load data into database."""
    pipeline = COVID19Pipeline()

    # Pull from XCom
    transformed_data = context["task_instance"].xcom_pull(
        task_ids="transform_data", key="transformed_data"
    )

    pipeline.run_load(transformed_data)

    # Cleanup
    pipeline.loader.close()

    return "Data loaded successfully"


def send_notification(**context):
    """Task: Send notification on completion."""
    # Get execution stats
    dag_run = context["dag_run"]
    execution_date = dag_run.execution_date

    print(f"✓ Pipeline completed successfully at {execution_date}")
    print("=" * 60)
    print("COVID-19 ETL Pipeline - Execution Summary")
    print("=" * 60)
    print("Status: SUCCESS")
    print(f"Execution Date: {execution_date}")
    print("=" * 60)

    # In production, send email/Slack notification here
    return "Notification sent"


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Define DAG
dag = DAG(
    "covid19_etl_pipeline",
    default_args=default_args,
    description="Daily COVID-19 data ETL pipeline with PySpark",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["covid19", "etl", "pyspark", "health"],
    max_active_runs=1,
)

# Define tasks
extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

notify_task = PythonOperator(
    task_id="send_notification",
    python_callable=send_notification,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> notify_task
