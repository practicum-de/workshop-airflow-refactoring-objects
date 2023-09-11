import csv
import datetime as dt
import logging

import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def download_titanic_dataset():
    logging.info("Downloading titanic dataset")

    url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
    conn = BaseHook.get_connection("POSTGRES_DB")

    with requests.Session() as s:
        download = s.get(url)

    decoded_content = download.content.decode("utf-8")

    cr = csv.reader(decoded_content.splitlines(), delimiter=",")
    my_list = list(cr)
    for row in my_list:
        print(row)

    logging.info("Downloaded titanic dataset")


dag = DAG(
    dag_id="titanic_dag_req",
    schedule_interval="0/15 * * * *",
    start_date=dt.datetime(2023, 9, 1),
    catchup=False,
    tags=["demo", "stg", "cdm", "titanic"],
    is_paused_upon_creation=False,
    default_args=args,
)


start = BashOperator(
    task_id="start",
    bash_command='echo "Here we start! "',
    dag=dag,
)

create_titanic_dataset = PythonOperator(
    task_id="download_titanic_dataset",
    python_callable=download_titanic_dataset,
    dag=dag,
)

titanic_sex_dm = PostgresOperator(
    task_id="create_titanic_sex_dm",
    postgres_conn_id="PG_CONN",
    sql="""
            CREATE TABLE public.titanic_sex_dm AS
            SELECT
                t."Sex"                     AS "sex",
                count(DISTINCT t."Name")    AS name_uq,
                avg("Age")                  AS age_avg,
                sum("Fare")                 AS fare_sum
            FROM public.titanic t
            GROUP BY t."Sex"
          """,
)


start >> create_titanic_dataset >> titanic_sex_dm
