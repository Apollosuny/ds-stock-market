import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from plugins.models.initialize import initialize_db


@dag(
    "initial-db",
    default_args={
        "owner": "Trung",
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    description="DAG for creating database and tables",
    start_date=datetime.datetime(2024, 11, 10),
    schedule_interval=None,
    catchup=False,
    tags=["initial-db"],
)
def initialize():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    initialize_task = PythonOperator(
        task_id="initialize_database",
        python_callable=initialize_db,
    )

    start >> initialize_task >> end


initialize()
