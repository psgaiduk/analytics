FROM apache/airflow:2.8.4

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false

COPY entrypoint-airflow.sh /entrypoint-airflow.sh

ENTRYPOINT ["bash", "/entrypoint-airflow.sh"]
