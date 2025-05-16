FROM apache/airflow:latest-python3.9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.7.9 \
    dbt-postgres==1.7.9 \
    astronomer-cosmos[dbt-postgres]==1.2.1

ENV DBT_PROJECT_DIR=/opt/airflow/dbt
RUN mkdir -p $DBT_PROJECT_DIR

COPY ./dbt /opt/airflow/dbt
WORKDIR /opt/airflow
