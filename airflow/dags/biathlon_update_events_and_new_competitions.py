from datetime import datetime
from logging import getLogger

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

from choices.name_tables import TableNames
from functions.generate_season_id import generate_season_id_func
from sdk.clickhouse_sdk import GetDataByQuery


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_update_events_and_new_competitions",
    schedule="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon", "regular"],
    max_active_tasks=5,
) as dag:

    @task()
    def get_season_id(**kwargs) -> list:
        """
        Get SeasonId for biathlonresults.com from params or generate it if not provided.

        Returns:
            list: List of dictionaries with SeasonId for biathlonresults.com, e.g. [{"season_id": "2324"}]
        """
        season_id = generate_season_id_func()
        log.info(f"SeasonId для обновления: {season_id}")
        return [{"season_id": season_id}]

    @task()
    def get_events_ids() -> list:
        """
        Get EventIds for today competition.

        Returns:
            list: List of dictionaries with EventIds, e.g. [{"event_id": "123"}]

        """
        query_for_get_events_ids = f"""
        SELECT
            EventId
        FROM {TableNames.BIATHLON_EVENTS.value}
        WHERE today() BETWEEN parseDateTime64BestEffort(StartDate) AND parseDateTime64BestEffort(EndDate)
        """
        log.info(f"Query for get events ids: {query_for_get_events_ids}")
        events_df = GetDataByQuery().get_data(query=query_for_get_events_ids)
        log.info(f"Get events from database events_df = {events_df.head()}")
        if events_df.empty:
            log.info("No events found today. Skipping downstream tasks.")
            raise AirflowSkipException("No events to process")
        return [{"event_id": event_id} for event_id in events_df["EventId"].tolist()]

    season_id = get_season_id()

    trigger_events = TriggerDagRunOperator.partial(
        task_id="trigger_biathlon_events_new",
        trigger_dag_id="biathlon_events",
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=1,
    ).expand(conf=season_id)

    events_ids = get_events_ids()
    trigger_events >> events_ids

    TriggerDagRunOperator.partial(
        task_id="trigger_biathlon_competitions_new",
        trigger_dag_id="biathlon_competitions",
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=5,
    ).expand(conf=events_ids)
