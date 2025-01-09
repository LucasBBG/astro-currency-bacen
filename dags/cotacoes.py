from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator #operador para criar a tabela no postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook #operador para fazer o UPSERT (Update/Insert) no postgres

import pandas as pd #se for um volume grande de dados não usar pandas, usar pyspark
import requests
import logging

from io import StringIO

dag = DAG(
    'fin_cotacoes_bcb_classic',
    schedule_interval = '@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1, #se a tarefa falhar quantas tentativas ele vai fazer antes de dizer que falhou
        'start_date': datetime(2024, 1, 1),
        'catchup': False, #se tiver true vai fazer o processo para todos os dias da data de inicio ate hoje
    },
    tags=["bcb"]
)