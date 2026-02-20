from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from choices.name_tables import TableNames
from sdk.clickhouse_sdk import GetDataByQuery


log = getLogger(__name__)


def generate_season_id():
    query_for_get_race_id = f"""
    SELECT
        season_id
    FROM {TableNames.BIATHLON_COMPETITION.value}
    GROUP BY season_id
    ORDER BY MIN(updated_at)
    LIMIT 1
    """
    season_id_df = GetDataByQuery().get_data(query=query_for_get_race_id)
    log.info(f"Get oldest season_id from database season_id_df = {season_id_df.head()}")
    return season_id_df["season_id"].iloc[0]


with DAG(
    dag_id="biathlon_update_oldest_season",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon", "regular"],
) as dag:

    @task()
    def build_conf():
        return {
            "season_id": generate_season_id(),
        }

    conf_data = build_conf()

    trigger = TriggerDagRunOperator(
        task_id="trigger_update_season_results_oldest",
        trigger_dag_id="biathlon_update_season_results",
        conf=conf_data,
    )
