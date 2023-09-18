import datetime as dt
import logging

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dwh.airflow.config import AppConfig
from dwh.core.adapters.titanic_passenger_api_adapter import TitanicPassengerApiAdapter
from dwh.core.domain.load_passengers_job import LoadPassengersJob
from dwh.core.repository.titanic_passenger_clickhouse_repository import TitanicPassengerClickhouseRepository

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def download_titanic_dataset():
    logging.info("Downloading titanic dataset")

    adapter = TitanicPassengerApiAdapter(AppConfig.titanic_api_url())

    repository = TitanicPassengerClickhouseRepository()

    job = LoadPassengersJob(adapter, repository)
    job.execute()

    logging.info("Downloaded titanic dataset")


dag = DAG(
    dag_id="titanic_load_clickhouse_dag",
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


start >> create_titanic_dataset
