from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import time


def sleep_function(time):
    time = time * 60
    time.sleep(time)


REGIONS = ["uk"]
for region in REGIONS:
    dag_id = f"long_dag_{region}"
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
        dummy_op1 = DummyOperator(
            task_id="DummyOperator1",
        )

        sleep_task = PythonOperator(
            task_id="sleep_task",
            python_callable=sleep_function,
            op_args=[180],
        )

        dummy_op1 >> sleep_task
