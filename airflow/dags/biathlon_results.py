from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, Param, task

from functions.insert_values_to_database import load_to_database
from sdk.biathlon.fetch_data import BiathlonResultsFetcher
from sdk.clickhouse_sdk import DeleteFromDatabase


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_update_race_results",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon"],
    params={
        "rt": Param(
            385698,
            type="integer",
            description="RT для biathlonresults.com",
        ),
        "race_id": Param(
            type="string",
            description="Id гонки, которую нужно обновить.",
        ),
    },
) as dag:

    @task()
    def update_race_results(**kwargs) -> list:
        """Загружает данные соревнований с biathlonresults.com"""
        race_id = kwargs["params"]["race_id"]
        rt = kwargs["params"]["rt"]
        recreate = False

        table_results = "biathlon_raw.result"
        table_analytics_results = "biathlon_raw.analytics_result"
        DeleteFromDatabase(table_name=table_results).delete_where(condition=f"race_id = '{race_id}'")
        DeleteFromDatabase(table_name=table_analytics_results).delete_where(condition=f"race_id = '{race_id}'")

        results, analytics_results = BiathlonResultsFetcher().fetch(race_id=race_id, rt=rt)
        load_to_database(table_name=table_results, data=results, recreate=recreate)
        load_to_database(table_name=table_analytics_results, data=analytics_results, recreate=recreate)

    update_race_results()
