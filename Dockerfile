FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y build-essential default-libmysqlclient-dev

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./dags /opt/airflow/dags
