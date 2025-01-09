from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator #operador para criar a tabela no postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook #operador para fazer o UPSERT (Update/Insert) no postgres

import pandas as pd #se for um volume grande de dados não usar pandas, usar pyspark
import requests
import logging
from datetime import datetime
from io import StringIO

#Defining DAG
dag_bacen = DAG(
    'daily_currency_bacen',
    schedule_interval = '@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1, #se a tarefa falhar quantas tentativas ele vai fazer antes de dizer que falhou
        'start_date': datetime(2024, 1, 1),
        'catchup': False, #se tiver true vai fazer o processo para todos os dias da data de inicio ate hoje
    },
    tags=["bacen"]
)

#Creating table
create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_currency_bacen (
        data_fechamento DATE,
        cod VARCHAR(10),
        tipo VARCHAR(10),
        desc_moeda VARCHAR(100),
        taxa_compra NUMERIC(10, 2),
        taxa_venda NUMERIC(10, 2),
        paridade_compra REAL,
        paridade_venda REAL,
        processed_at TIMESTAMP,
        constraint pk_daily_currency_bacen primary key (data_fechamento, cod)
    );
"""

create_table_task = PostgresOperator(
    task_id='create_table_postgres_task',
    postgres_conn_id='postgres_astro',
    sql=create_table_sql,
    dag=dag_bacen
)

#Extracting data
def extract(**kwargs):
    ds_nodash = kwargs["ds_nodash"] #data (sem a barra = nodash) de execução da pipeline (agendamento do dia 1, dia 2, ... mesmo que execute o script hoje) 
    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = f"{base_url}{ds_nodash}.csv"
    logging.warning(f"URL: {full_url}")

    try:
        response = requests.get(full_url)
        if response.status_code == 200:
            csv_data = response.content.decode("utf-8")
            return csv_data
    except Exception as e:
        logging.error(f"Erro ao extrair dados: {e}")

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    provide_context=True,
    dag=dag_bacen
)

#Transforming data
def transform(**kwargs):
    currency_data = kwargs["ti"].xcom_pull(task_ids="extract_task")
    columns = [
        "data_fechamento",
        "cod",
        "tipo",
        "desc_moeda",
        "taxa_compra",
        "taxa_venda",
        "paridade_compra",
        "paridade_venda"
    ]
    columns_types = {
        "data_fechamento": str,
        "cod": str,
        "tipo": str,
        "desc_moeda": str,
        "taxa_compra": float,
        "taxa_venda": float,
        "paridade_compra": float,
        "paridade_venda": float
    }
    parse_date = ["data_fechamento"]
    if currency_data:
        df = pd.read_csv(
            StringIO(currency_data),
            sep=";",
            decimal=",",
            thousands=".",
            encoding="utf-8",
            header=None,
            names=columns,
            dtype=columns_types,
            parse_dates=parse_date
        )
        df['processed_at'] = datetime.now()
        return df
    
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag_bacen
)


#Loading data
def load(**kwargs):
    currency_data = kwargs["ti"].xcom_pull(task_ids="transform_task")
    if currency_data is not None:
        hook = PostgresHook(postgres_conn_id='postgres_astro', schema='astro')
        rows = list(currency_data.itertuples(index=False, name=None))
        hook.insert_rows(
            table="daily_currency_bacen",
            rows=rows,
            replace=True,
            replace_index=['data_fechamento', 'cod'],
            target_fields=['data_fechamento', 'cod', 'tipo', 'desc_moeda', 'taxa_compra', 'taxa_venda', 'paridade_compra', 'paridade_venda', 'processed_at']
        )
        