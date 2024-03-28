FROM python:3.10
FROM apache/airflow:2.3.3

ADD webserver_config.py /opt/airflow/webserver_config.py

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

WORKDIR /app
COPY requirements.txt .
USER airflow
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
# CMD [ "python", "-u","ETL_Polygol.py" ]