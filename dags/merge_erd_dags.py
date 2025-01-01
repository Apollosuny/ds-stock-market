from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from plugins.transform_erd.run import MERGE_ERD

# Định nghĩa một số tham số chung cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    'catchup': False  
}

def run_merge_erd_transform(last_timestamp=None):
    logger = logging.getLogger('airflow')
    merge_erd = MERGE_ERD(logger)
    merge_erd.transform(last_timestamp)

with DAG(
    'merge_erd_dag',  # Tên DAG
    default_args=default_args,
    description='DAG để chạy biến đổi dữ liệu cho ERD',
    schedule_interval=timedelta(days=1), 
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id='run_merge_erd_transform',
        python_callable=run_merge_erd_transform,
        op_args=[None], 
        dag=dag,
    )


