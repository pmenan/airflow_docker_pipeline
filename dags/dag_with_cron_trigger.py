from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator


default_args={
    'owner' : 'N\'Dri MENAN',
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2)
}
with DAG(
    dag_id='dag_with_cron_trigger',
    description='ceci est mon dag avec cron trigger',
    default_args=default_args,
    start_date=datetime(2024,5,10),
    # schedule_interval="5 4 * * wed" # exécution tous les mercredi à 04:05:00
    schedule_interval="5 4 * * wed-fri" # exécution tous les mercredi, jeudi et vendredi à 04:05:00
    # schedule_interval="5 4 * * wed,thu,fri" # exécution tous les mercredi, jeudi et vendredi à 04:05:00


) as dag:
    task=BashOperator(
        task_id = 'task1',
        bash_command="echo ceci est un cron trigger"
    )

task