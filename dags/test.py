from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import time


def sleep_function(sleep_time):
    time.sleep(sleep_time * 60)


REGIONS = ["uk", "qa", "prod-uk", "prod-us", "dev"]
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
        schedule_interval="@hourly",
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
            op_args=[20],
        )

        dummy_op2 = DummyOperator(
            task_id="DummyOperator2",
        )

        sleep_task2 = PythonOperator(
            task_id="sleep_task2",
            python_callable=sleep_function,
            op_args=[10],
        )

        dummy_op3 = DummyOperator(
            task_id="DummyOperator3",
        )

        dummy_op1 >> sleep_task >> dummy_op2 >> sleep_task2 >> dummy_op3
