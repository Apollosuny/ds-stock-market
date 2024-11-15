from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import datetime


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

    create_db = BashOperator(
        task_id="create_database",
        bash_command='cd /opt/project && echo "Current directory: $(pwd)" && echo "Contents of /opt/project:" && ls -l /opt/project && python -m models.initialize',
        do_xcom_push=True,
    )

    create_db


initialize()
