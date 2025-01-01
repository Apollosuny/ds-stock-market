import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from plugins.models.initialize import initialize_db
# from plugins.transform.transform_product_data import transform_product_data


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
    initialize_task = PythonOperator(
        task_id="initialize_database",
        python_callable=initialize_db,
    )

    # transform_product_data_task = PythonOperator(
    #     task_id="transform_product_data",
    #     python_callable=transform_product_data,
    # )

    initialize_task


initialize()
