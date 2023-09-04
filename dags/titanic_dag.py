import datetime as dt
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

from sqlalchemy import create_engine

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2020, 12, 23),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "pause": False,
}


def download_titanic_dataset():
    url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"
    conn = BaseHook.get_connection("POSTGRES_DB")
    engine = create_engine(conn.get_uri())

    df = pd.read_csv(url)
    df.to_sql("titanic", engine, index=False, if_exists="replace", schema="public")


def pivot_dataset():
    engine = create_engine("postgresql+psycopg2://airflow:airflow@host.docker.internal:5432/de")

    titanic_df = pd.read_sql("select * from public.titanic", con=engine)
    df = titanic_df.pivot_table(index=["Sex"], columns=["Pclass"], values="Name", aggfunc="count").reset_index()
    df.to_sql("titanic_pivot", engine, index=False, if_exists="replace", schema="public")


dag = DAG(
    dag_id="titanic_pivot",
    schedule_interval=None,
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

pivot_titanic_dataset = PythonOperator(
    task_id="pivot_dataset",
    python_callable=pivot_dataset,
    dag=dag,
)


start >> create_titanic_dataset >> pivot_titanic_dataset
