import datetime as dt
import logging

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from dwh.airflow.config import AppConfig
from dwh.core.adapters.titanic_passenger_api_adapter import TitanicPassengerApiAdapter
from dwh.core.domain.load_passengers_job import LoadPassengersJob
from dwh.core.repository.titanic_passenger_repository import TitanicPassengerPsycopgRepository

args = {
    "owner": "airflow",
    "start_date": dt.datetime(2023, 9, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


def download_titanic_dataset():
    logging.info("Downloading titanic dataset")

    adapter = TitanicPassengerApiAdapter(AppConfig.titanic_api_url())

    conn = AppConfig.titanic_raw_repository()
    repository = TitanicPassengerPsycopgRepository(conn)

    job = LoadPassengersJob(adapter, repository)
    job.execute()

    logging.info("Downloaded titanic dataset")


dag = DAG(
    dag_id="titanic_dag",
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
            DROP TABLE IF EXISTS public.titanic_sex_dm;

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
