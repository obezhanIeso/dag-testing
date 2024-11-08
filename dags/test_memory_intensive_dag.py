from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import numpy as np


def memory_intensive_task():
    # Create a large array to consume a significant amount of memory
    large_array = np.random.random((100000, 100000))  # Adjust the size to fit your available memory
    result = np.sum(large_array)
    print(f"Sum of large array: {result}")


REGIONS = ["test-uk"]
for region in REGIONS:
    dag_id = f"test_memory_intensive_{region}"
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
            "retry_delay": timedelta(minutes=30),
        },
    ) as dag:
        dummy_op1 = DummyOperator(
            task_id="DummyOperator1",
        )

        task = PythonOperator(
            task_id='memory_intensive_task',
            python_callable=memory_intensive_task,
        )

        # task2 = PythonOperator(
        #     task_id='memory_intensive_task2',
        #     python_callable=memory_intensive_task,
        # )

        # task3 = PythonOperator(
        #     task_id='memory_intensive_task3',
        #     python_callable=memory_intensive_task,
        # )

        # task4 = PythonOperator(
        #     task_id='memory_intensive_task4',
        #     python_callable=memory_intensive_task,
        # )
        #
        # task5 = PythonOperator(
        #     task_id='memory_intensive_task5',
        #     python_callable=memory_intensive_task,
        # )
