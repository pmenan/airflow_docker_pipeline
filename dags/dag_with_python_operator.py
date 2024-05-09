from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


default_args = {
    'owner' : "N'Dri MENAN",
    'email' : ['menanpatrick49@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 2,
    'depends_on_pass' : False,
    'retries_delay' : timedelta(minutes=5)
}

def hello(name:str, age:int):
    print(f"Salut à tous, je m'appelle {name}, et j'ai {age} ans et j'utilise docker et s'est fun !")

def hello_v2(age:int, ti):
    """
    affiche le nom stocké dans les xcoms variables et l'age indiqué dans la fonction
    @age : age
    @ti : paramètre dans les xcoms
    """
    name = ti.xcom_pull(task_ids = 'getName')
    print(f"Salut à tous, je m'appelle {name}, j'ai {age} ans et j'utilise docker et s'est fun !")

def hello_v3(age:int, ti):
    """
    affiche le nom stocké dans les xcoms variables et l'age indiqué dans la fonction
    @age : age
    @ti : paramètre permettant de récupérer les valeurs dans variables stockées dans xcom airflow
    """
    nom = ti.xcom_pull(key='nom', task_ids = 'getName_v2')
    prenom = ti.xcom_pull(key='prenom', task_ids = 'getName_v2')
    print(f"Salut à tous, je m'appelle {nom} {prenom} j'ai {age} ans. J'utilise docker et s'est fun !")

def getName():
    return "N'Dri"

def getName_v2(ti):
    """
    push les valeurs des paramètres nom et prénom dans xcom airflow
    """
    ti.xcom_push(key='nom', value='MENAN'),
    ti.xcom_push(key='prenom', value='N\'Dri')



with DAG(
    dag_id='my_python_operators_dag_v05',
    description= "Dag airflow pour pratiquer les python operators",
    default_args=default_args,
    start_date=datetime(2024, 5, 9, 6),
    schedule_interval='@daily'

) as dag:
    task1=PythonOperator(
        task_id = 'hello_v3',
        python_callable=hello_v3,
        op_kwargs={'age' :'20'}
    )

    task2=PythonOperator(
        task_id='getName_v2',
        python_callable=getName_v2
    )

    task2 >> task1
    