from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="global_sales_etl_pipeline",
    default_args=default_args,
    description="Global Sales Retail Analytics ETL Pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["databricks", "retail", "etl"],
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_layer",
        bash_command="echo 'Trigger Bronze Layer Notebook'",
    )

    silver_task = BashOperator(
        task_id="silver_layer",
        bash_command="echo 'Trigger Silver Layer Notebook'",
    )

    gold_task = BashOperator(
        task_id="gold_layer",
        bash_command="echo 'Trigger Gold Layer Notebook'",
    )

    bronze_task >> silver_task >> gold_task