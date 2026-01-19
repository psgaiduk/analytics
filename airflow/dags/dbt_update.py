from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

from sdk.clickhouse_sdk import TableDropper


with DAG(
    dag_id="dbt_run",
    schedule="0 */1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt"],
) as dag:

    @task
    def drop_old_tables():
        TableDropper().drop_all_tables_in_db(db_name="biathlon")

    drop_tables = drop_old_tables()

    dbt_run = BashOperator(
        task_id="dbt_run", bash_command="cd /opt/airflow/dbt && dbt deps && dbt run --profiles-dir ."
    )

    drop_tables >> dbt_run
