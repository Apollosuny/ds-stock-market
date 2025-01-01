from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from plugins.transfrom_olap.run import MERGE_OLAP

# Định nghĩa một số tham số chung cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
    'catchup': False 
}

def run_merge_olap_transform(last_timestamp=None):
    logger = logging.getLogger('airflow')
    merge_erd = MERGE_OLAP(logger)
    merge_erd.transform(last_timestamp)

# Tạo DAG
with DAG(
    'merge_olap_dag', 
    default_args=default_args,
    description='DAG để chuyển đổi dữ liệu từ ERD sang OLAP',
    schedule_interval=timedelta(days=1),  
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id='run_merge_olap_transform',
        python_callable=run_merge_olap_transform,
        op_args=[None], 
        dag=dag,
    )


