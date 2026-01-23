from datetime import datetime
from logging import getLogger

from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, task

from choices.name_tables import TableNames
from sdk.clickhouse_sdk import GetDataByQuery


log = getLogger(__name__)


with DAG(
    dag_id="biathlon_update_fresh_race",
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
    },
) as dag:

    @task()
    def get_race_id() -> str:
        log.info("Get finished race without results")
        query_for_get_race_id = f"""
        SELECT
            RaceId
        FROM {TableNames.BIATHLON_COMPETITION.value}
        WHERE StatusId = '11'
        AND RaceId NOT IN (
            SELECT
                DISTINCT race_id
            FROM {TableNames.BIATHLON_RESULT.value}
        )
        ORDER BY StartTime DESC
        LIMIT 1
        """
        race_id_df = GetDataByQuery().get_data(query=query_for_get_race_id)
        if race_id_df.empty:
            log.warning("Race Id is empty")
            raise AirflowSkipException("No finish races found without results")

        log.info(f"Get race id from database race_id_df = {race_id_df}")

        return race_id_df["RaceId"].iloc[0]

    race_id = get_race_id()

    TriggerDagRunOperator(
        task_id="trigger_single_race_update",
        trigger_dag_id="biathlon_update_race_results",
        conf={"race_id": race_id},
        wait_for_completion=True,
    )
