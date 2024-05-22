"""ETL Helper script to demonstrate using Apache Airflow to schedule tasks."""
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from database_to_database import copy_readings


def copy_readings_with_args(**kwargs) -> None:
    # Set arguments for copy_readings from context
    start = kwargs.get("prev_execution_date")
    end = kwargs.get("execution_date")
    copy_readings(start, end)


dag = DAG(
    "readings",
    schedule_interval=dt.timedelta(days=1),
    start_date=dt.datetime(2019, 8, 1),
    catchup=True,
)

t1 = PythonOperator(
    task_id="copy_readings",
    python_callable=copy_readings_with_args,
    provide_context=True,
    dag=dag,
)
