from datetime import datetime
from logging import getLogger

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

from choices import CompetitionTable, EventsTable
from choices.name_tables import TableNames
from constants import COMPETITION_FINISHED_STATUS
from sdk.clickhouse_sdk import GetDataByQuery


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_update_finished_competitions",
    schedule="0 */1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon", "regular"],
    max_active_tasks=5,
) as dag:

    @task()
    def get_events_ids() -> list:
        """
        Get EventIds for given SeasonId from database.

        Returns:
            list: List of dictionaries with EventIds, e.g. [{"event_id": "123"}]

        """
        query_for_get_events_ids = f"""
        SELECT
            {CompetitionTable.EVENT_ID.value} AS event_id
        FROM {TableNames.BIATHLON_COMPETITION.value}
        WHERE {CompetitionTable.EVENT_ID.value} IN (
            SELECT
                {EventsTable.EVENT_ID.value}
            FROM {TableNames.BIATHLON_EVENTS.value}
            WHERE today() BETWEEN parseDateTime64BestEffort({EventsTable.START_DATE.value})
                AND parseDateTime64BestEffort({EventsTable.END_DATE.value})
        )
        AND DATE(parseDateTime64BestEffort({CompetitionTable.START_TIME.value})) = today()
        AND parseDateTime64BestEffort({CompetitionTable.START_TIME.value}) <= now()
        AND {CompetitionTable.STATUS_ID.value} != '{COMPETITION_FINISHED_STATUS}'
        """
        log.info(f"Query for get events ids: {query_for_get_events_ids}")
        events_df = GetDataByQuery().get_data(query=query_for_get_events_ids)
        log.info(f"Get events from database events_df = {events_df.head()}")
        if events_df.empty:
            log.info("No events found today. Skipping downstream tasks.")
            raise AirflowSkipException("No events to process")
        return [{"event_id": event_id} for event_id in events_df["event_id"].tolist()]

    events_ids = get_events_ids()

    TriggerDagRunOperator.partial(
        task_id="trigger_biathlon_competitions_finished",
        trigger_dag_id="biathlon_competitions",
        wait_for_completion=True,
        poke_interval=5,
        max_active_tis_per_dag=5,
    ).expand(conf=events_ids)
