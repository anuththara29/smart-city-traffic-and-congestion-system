# peak_hour_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('peak_traffic_hour',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    run_daily_peak = BashOperator(
        task_id='run_daily_peak',
        bash_command='python3 /opt/airflow/dags/scripts/daily_peak_hour.py {{ ds }}'
    )
