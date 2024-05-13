# import des lib nécessaires au bon fonctionnement du programme
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner' : "N'Dri MENAN",
    'depends_on_pass' : False,
    'email' : ['menanpatrick49@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries' : 3,
    'retries_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id='my_firt_dag_v5',
    description='ceci est mon 1er dag construit avec airflow pour ce projet',
    default_args=default_args,
    start_date=datetime(2024, 5, 9, 6), # exécuter mon DAG à partir du 9 mai 2024 etb tous les jours à 6h du matin.
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello ceci est ma prémière bash task!"
    )

    task2 = BashOperator(
        task_id='seconde_task',
        bash_command="echo je suis la tâche 2, je me run après la tâche 1"
    )
    
    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo Je suis la 3eme tâche, je me run après la tâche 1 et en même temps que la tâche 2"
    )

    # exécution des tasks : méthode 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # exécution des tasks : méthode 2
    # task1 >> task2
    # task1 >> task3

    # exécution des tasks : méthode 3
    task1 >> [task2, task3]
