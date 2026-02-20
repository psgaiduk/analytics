from datetime import datetime
from logging import getLogger

from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, Param, task
from pandas import DataFrame

from choices.name_tables import TableNames
from functions.generate_season_id import generate_season_id_func
from functions.insert_values_to_database import load_to_database
from sdk.biathlon.fetch_data import BiathlonEventsFetcher
from sdk.clickhouse_sdk import DeleteFromDatabase


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_events",
    schedule="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon"],
    params={
        "season_id": Param(
            "",
            type="string",
            description="SeasonId для biathlonresults.com",
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
    def get_events(**kwargs) -> DataFrame:
        rt = kwargs["params"]["rt"]
        season_id = kwargs["params"]["season_id"] or generate_season_id_func()
        events_df = BiathlonEventsFetcher(rt=rt, season_id=season_id).fetch()
        if events_df.empty:
            log.info("Данных нет. Пропускаем выполнение всего DAG.")
            raise AirflowSkipException("No events found for this season")
        events_df = events_df[
            (events_df["EventId"].str.startswith(f"BT{season_id}SWRL")) & (events_df["Level"].astype("int") == 1)
        ]
        return events_df

    @task()
    def load_events_to_clickhouse(events_df, **kwargs) -> None:
        season_id = kwargs["params"]["season_id"] or generate_season_id_func()
        table_name = TableNames.BIATHLON_EVENTS.value
        DeleteFromDatabase(table_name=table_name).delete_where(condition=f"SeasonId = '{season_id}'")
        load_to_database(data=events_df, table_name=table_name)

    events = get_events()
    load_events_to_clickhouse(events_df=events)
