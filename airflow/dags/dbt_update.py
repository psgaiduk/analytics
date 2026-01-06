from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="dbt_run",
    schedule="0 */1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run", bash_command="cd /opt/airflow/dbt && dbt deps && dbt run --profiles-dir ."
    )

    dbt_run