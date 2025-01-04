import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.transfrom_olap.run import MERGE_OLAP

# Định nghĩa một số tham số chung cho DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


def run_merge_olap_transform_dim(last_timestamp=None):
    logger = logging.getLogger("airflow")
    merge_erd = MERGE_OLAP(logger)
    merge_erd.transform_dim(last_timestamp)


def run_merge_olap_transform_fact(last_timestamp=None):
    logger = logging.getLogger("airflow")
    merge_erd = MERGE_OLAP(logger)
    merge_erd.transform_fact(last_timestamp)


# Tạo DAG
with DAG(
    "merge_olap_dag",
    default_args=default_args,
    description="DAG để chuyển đổi dữ liệu từ ERD sang OLAP",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["merge_olap"],
) as dag:
    transform_dim_task = PythonOperator(
        task_id="run_merge_olap_transform_dim",
        python_callable=run_merge_olap_transform_dim,
        op_args=[None],
        dag=dag,
    )

    transform_fact_task = PythonOperator(
        task_id="run_merge_olap_transform_fact",
        python_callable=run_merge_olap_transform_fact,
        op_args=[None],
        dag=dag,
    )

    transform_dim_task >> transform_fact_task
