from datetime import date, datetime
from logging import getLogger

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, task
from pandas import DataFrame, concat

from choices.name_tables import TableNames
from functions.insert_values_to_database import load_to_database
from sdk.biathlon.fetch_data import BiathlonCompetitionsFetcher, BiathlonEventsFetcher
from sdk.clickhouse_sdk import DeleteFromDatabase


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_season_results",
    schedule="0 1 * * *",
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
        season_id = kwargs["params"]["season_id"] or generate_season_id()
        events_df = BiathlonEventsFetcher(rt=rt, season_id=season_id).fetch()
        if events_df.empty:
            log.info("Данных нет. Пропускаем выполнение всего DAG.")
            raise AirflowSkipException("No events found for this season")
        return events_df

    @task()
    def load_events_to_clickhouse(events_df, **kwargs) -> list[int]:
        season_id = kwargs["params"]["season_id"] or generate_season_id()
        table_name = TableNames.BIATHLON_EVENTS.value
        DeleteFromDatabase(table_name=table_name).delete_where(condition=f"SeasonId = '{season_id}'")
        load_to_database(data=events_df, table_name=table_name)
        return [event_id for event_id in events_df["EventId"].tolist()]

    @task()
    def get_competitions(event_id, **kwargs):
        rt = kwargs["params"]["rt"]
        season_id = kwargs["params"]["season_id"] or generate_season_id()
        return BiathlonCompetitionsFetcher(rt=rt, season_id=season_id).fetch(event_id=event_id)

    @task()
    def merge_and_load_competitions(dfs_list, **kwargs):
        competitions_df = concat(dfs_list, ignore_index=True)
        table_name = TableNames.BIATHLON_COMPETITION.value
        load_to_database(data=competitions_df, table_name=table_name)

        finished_races = competitions_df[competitions_df["StatusId"].astype(int) == 11]
        log.info(f"Total races: {len(competitions_df)}, Finished races to trigger: {len(finished_races)}")
        return [{"race_id": race_id} for race_id in finished_races["RaceId"].tolist()]

    def generate_season_id() -> str:
        """
        Генерирует season_id по текущей дате.

        Returns:
            str: season_id
        """
        log.info("Generate season_id")
        today = date.today()
        year = today.year % 100
        log.debug(f"today = {today} year = {year}")
        if today.month < 7:
            log.debug("month less 7")
            season_id = f"{year - 1}{year}"
        else:
            log.debug("Month after 6")
            season_id = f"{year}{year + 1}"
        return season_id

    events_df = get_events()
    events_ids = load_events_to_clickhouse(events_df=events_df)
    dfs_list = get_competitions.partial(max_active_tis_per_dag=1).expand(event_id=events_ids)
    race_ids = merge_and_load_competitions(dfs_list=dfs_list)

    TriggerDagRunOperator.partial(
        task_id="trigger_update_races_results",
        trigger_dag_id="biathlon_update_race_results",
        wait_for_completion=True,
        max_active_tis_per_dag=1,
    ).expand(conf=race_ids)
