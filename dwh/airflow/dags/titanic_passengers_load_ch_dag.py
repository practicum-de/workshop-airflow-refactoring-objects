import datetime as dt
import logging

from airflow.decorators import dag, task

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


@dag(
    schedule_interval="0/15 * * * *",
    start_date=dt.datetime(2023, 9, 1),
    catchup=False,
    tags=["demo", "stg", "cdm", "titanic"],
    is_paused_upon_creation=False,
    default_args=args,
)
def titanic_load_clickhouse_dag():
    @task()
    def start():
        logging.info("Here we start!")

    @task()
    def download_titanic_dataset():
        adapter = TitanicPassengerApiAdapter(AppConfig.titanic_api_url())
        repository = TitanicPassengerClickhouseRepository(AppConfig.titanic_ch_repository())

        logging.info("Downloading titanic dataset")

        job = LoadPassengersJob(adapter, repository)
        job.execute()

        logging.info("Downloaded titanic dataset")

    start() >> download_titanic_dataset()  # type: ignore


titanic_load_clickhouse = titanic_load_clickhouse_dag()
