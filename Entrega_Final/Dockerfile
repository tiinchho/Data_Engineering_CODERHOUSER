FROM apache/airflow:2.3.3

WORKDIR /app
ADD webserver_config.py /opt/airflow/webserver_config.py
COPY requirements.txt .

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt