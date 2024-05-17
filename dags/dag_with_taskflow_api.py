from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task


default_args={
    'owner' : 'N\'Dri MENAN',
    'depends_on_pass' : False,
    'email' : ['menanpatrick49@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 3,
    'retries_delay' : timedelta(minutes=2)
}

@dag(
    dag_id='dag_with_taskflow_api',
    description="ceci est mon dag avec utilisation de l'api taskflow",
    default_args=default_args,
    start_date=datetime(2024, 5, 10, 6),
    schedule_interval='@daily'
)

def hello_everyboby_etl():

    @task(multiple_outputs=True)
    def getName():
        """
        return un dictionnaire python constitué du nom et prenom
        """
        return{
            'first_name' : 'N\'Dri',
            'last_name' : 'MENAN'
        }
    
    @task()
    def get_age():
        """
        retourn la constante age
        """
        return 26
    
    @task()
    def get_info(first_name:str, last_name:str, age:int):
        """
        affiche l'ensemble des infos
        """
        print(f"Salut à tous, je suis {first_name} {last_name}, j'ai {age} ans et j'adore construire des datas pipeline")
    
    #appel au fonction
    name_dict = getName()
    age = get_age()
    get_info(name_dict['first_name'], name_dict['last_name'], age)

taskFlow_api_dag = hello_everyboby_etl()



