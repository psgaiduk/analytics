from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, Param, task

from choices.name_tables import TableNames
from functions.insert_values_to_database import load_to_database
from sdk.biathlon.fetch_data import BiathlonCompetitionsFetcher
from sdk.clickhouse_sdk import DeleteFromDatabase


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_competitions",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon", "triggered"],
    params={
        "event_id": Param(
            "",
            type="string",
            description="EventId для biathlonresults.com",
        ),
        "rt": Param(
            385698,
            type="integer",
            description="RT для biathlonresults.com",
        ),
    },
    max_active_tasks=5,
) as dag:

    @task()
    def get_competitions(**kwargs):
        rt = kwargs["params"]["rt"]
        event_id = kwargs["params"]["event_id"]
        season_id = event_id[2:6]
        return BiathlonCompetitionsFetcher(rt=rt, season_id=season_id).fetch(event_id=event_id)

    @task()
    def merge_and_load_competitions(competitions_df, **kwargs):
        event_id = kwargs["params"]["event_id"]
        table_name = TableNames.BIATHLON_COMPETITION.value
        DeleteFromDatabase(table_name=table_name).delete_where(condition=f"event_id = '{event_id}'")
        load_to_database(data=competitions_df, table_name=table_name)
        return

    competitions_df = get_competitions()
    merge_and_load_competitions(competitions_df=competitions_df)
