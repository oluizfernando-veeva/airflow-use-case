from datetime import datetime, timedelta

from airflow import DAG
# Operadores são as "tarefas" do Airflow. Cada operador faz um tipo de coisa:
from airflow.operators.empty import EmptyOperator        # tarefa vazia, só pra marcar início/fim
from airflow.operators.python import BranchPythonOperator, PythonOperator  # executa função Python
from airflow.utils.trigger_rule import TriggerRule

from utils.extract import extract_adverse_events
from utils.transform import transform_data
from utils.load import load_to_postgres

default_args = {
    'owner': 'airflow',
    'retries': 1,                        # se falhar, tenta mais 1 vez
    'retry_delay': timedelta(minutes=5), # espera 5 min antes de tentar de novo
}


def branch_on_data(**context):
    # context['ti'] é o "task instance" — objeto com info da execução atual
    # xcom_pull busca o valor que a tarefa 'transform_data' retornou
    # XCom é o mecanismo do Airflow pra tarefas trocarem dados entre si
    data = context['ti'].xcom_pull(task_ids='transform_data')
    # retorna o nome da próxima tarefa a executar (é assim que o BranchOperator funciona)
    return 'load_to_postgres' if data else 'skip'


# DAG = o pipeline inteiro. É o "grafo" de tarefas que o Airflow vai orquestrar.
with DAG(
    dag_id='openfda_adverse_events',      # nome único que aparece na UI
    default_args=default_args,
    description='Hourly ingestion of FDA adverse event reports into PostgreSQL',
    start_date=datetime(2024, 1, 1),      # a partir de quando o DAG pode rodar
    schedule='@hourly',                   # roda 1x por hora automaticamente
    catchup=False,                        # não executa runs do passado ao ativar o DAG
    tags=['openfda', 'pharma', 'etl'],
) as dag:

    # PythonOperator executa uma função Python como uma tarefa do pipeline
    extract = PythonOperator(
        task_id='extract_data',            # nome da tarefa na UI
        python_callable=extract_adverse_events,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # BranchPythonOperator escolhe qual caminho seguir baseado no retorno da função
    branch = BranchPythonOperator(
        task_id='branch_on_data',
        python_callable=branch_on_data,
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # EmptyOperator não faz nada — serve como ponto de convergência no grafo
    skip = EmptyOperator(task_id='skip')

    end = EmptyOperator(
        task_id='end',
        # por padrão o Airflow só executa uma tarefa se TODAS as anteriores tiveram sucesso.
        # essa regra especial diz: executa se pelo menos 1 upstream teve sucesso (e nenhuma falhou).
        # necessário porque branch escolhe load OU skip, nunca os dois.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # >> define a ordem de execução (dependências entre tarefas)
    # [load, skip] significa que branch pode ir pra qualquer um dos dois
    extract >> transform >> branch >> [load, skip] >> end
