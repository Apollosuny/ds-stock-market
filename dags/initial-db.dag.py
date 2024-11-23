from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import datetime
from airflow.operators.python import PythonOperator
from plugins.models.initialize import Initialize


def initial_db():
    Initialize("postgresql+psycopg2://admin:123456789@localhost:5432/dw-amazon-sales")


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
        python_callable=initial_db,
    )

    initialize_task


initialize()
