from datetime import datetime
from logging import getLogger

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, task

from choices.name_tables import TableNames
from functions.generate_season_id import generate_season_id_func
from sdk.clickhouse_sdk import GetDataByQuery


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_update_season_results",
    schedule="0 1 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon", "regular"],
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
    def get_season_id(**kwargs) -> str:
        """
        Get SeasonId for biathlonresults.com from params or generate it if not provided.

        Returns:
            str: SeasonId for biathlonresults.com
        """
        season_id = kwargs["params"]["season_id"] or generate_season_id_func()
        log.info(f"SeasonId для обновления: {season_id}")
        return season_id

    @task()
    def prepare_conf(season_id):
        return [{"season_id": season_id}]

    @task()
    def get_events_ids(season_id: str) -> list:
        """
        Get EventIds for given SeasonId from database.

        Args:
            season_id (str): SeasonId for biathlonresults.com

        Returns:
            list: List of dictionaries with EventIds, e.g. [{"event_id": "123"}, {"event_id": "456"}]

        """
        query_for_get_events_ids = f"""
        SELECT
            EventId
        FROM {TableNames.BIATHLON_EVENTS.value}
        WHERE SeasonId = '{season_id}'
        """
        log.info(f"Query for get events ids: {query_for_get_events_ids}")
        events_df = GetDataByQuery().get_data(query=query_for_get_events_ids)
        log.info(f"Get events from database events_df = {events_df.head()}")
        return [{"event_id": event_id} for event_id in events_df["EventId"].tolist()]

    @task()
    def get_races_ids(season_id: str) -> list:
        """
        Get RaceIds for given SeasonId from database.

        Args:
            season_id (str): SeasonId for biathlonresults.com

        Returns:
            list: List of dictionaries with RacesIds.
        """
        query_for_get_races_ids = f"""
        SELECT
            RaceId
        FROM {TableNames.BIATHLON_COMPETITION.value}
        WHERE season_id = '{season_id}' AND StatusId = '11'
        """
        log.info(f"Query for get races ids: {query_for_get_races_ids}")
        races_df = GetDataByQuery().get_data(query=query_for_get_races_ids)
        log.info(f"Get races ids from database races_df = {races_df.head()}")
        return [{"race_id": race_id} for race_id in races_df["RaceId"].tolist()]

    season_id = get_season_id()
    conf = prepare_conf(season_id=season_id)

    trigger_events = TriggerDagRunOperator.partial(
        task_id="trigger_biathlon_events_season",
        trigger_dag_id="biathlon_events",
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=1,
    ).expand(conf=conf)

    events_ids = get_events_ids(season_id=season_id)
    trigger_events >> events_ids

    trigger_races = TriggerDagRunOperator.partial(
        task_id="trigger_biathlon_competitions_season",
        trigger_dag_id="biathlon_competitions",
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=5,
    ).expand(conf=events_ids)

    race_ids = get_races_ids(season_id=season_id)
    trigger_races >> race_ids

    TriggerDagRunOperator.partial(
        task_id="trigger_races_results_season",
        trigger_dag_id="biathlon_race_results",
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=2,
    ).expand(conf=race_ids)
