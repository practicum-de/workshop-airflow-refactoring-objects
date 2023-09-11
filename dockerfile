FROM apache/airflow:2.7.0

COPY ./dwh/requirements.txt /opt/airflow/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /opt/airflow/requirements.txt

