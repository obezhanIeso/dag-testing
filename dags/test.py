from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime, timedelta


REGIONS = ["uk"]
for region in REGIONS:
    dag_id = f"test_{region}"
    with DAG(
        dag_id=dag_id,
        description=f"""
           Test
        """,
        start_date=datetime(2024, 10, 9),
        max_active_runs=1,
        catchup=False,
        schedule_interval="@daily",
        default_args={
            "owner": "Oleg Bezhan",
            "depends_on_past": False,
            "retries": 1,
        },
    ) as dag:
        empty_task = EmptyOperator(task_id="empty_task")

        empty_task