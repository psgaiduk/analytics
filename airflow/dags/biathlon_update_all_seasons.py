from datetime import datetime
from logging import getLogger

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from choices.name_tables import TableNames
from functions.get_season_id import get_season_id_func
from sdk.clickhouse_sdk import TableDropper


log = getLogger(__name__)


def get_seasons_list() -> list[dict[str, int]]:
    """Generates a list of season IDs from 1957-1958 to the current season.

    Returns:
        list[dict[str, int]]: List of dictionaries containing season IDs.
    """

    season_ids = []

    this_year = datetime.now().year % 100

    for i in range(0, this_year + 1):
        season_ids.append({"season_id": get_season_id_func(year_end=i)})

    season_ids.reverse()

    for i in range(99, 56, -1):
        season_ids.append({"season_id": get_season_id_func(year_end=i)})

    log.info(f"Seasons to process: {[s['season_id'] for s in season_ids]}")
    return season_ids


@dag(
    dag_id="biathlon_update_all_seasons",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon"],
)
def biathlon_update_dag():

    @task()
    def drop_old_data():
        """Задача на удаление таблиц."""
        tables_to_remove = [
            TableNames.BIATHLON_COMPETITION.value,
            TableNames.BIATHLON_ANALYTICS_RESULT.value,
            TableNames.BIATHLON_RESULT.value,
            TableNames.BIATHLON_EVENTS.value,
        ]

        dropper = TableDropper()
        dropper.drop_tables(tables_to_remove)

    @task()
    def fetch_seasons():
        """Генерирует полный список сезонов."""
        return get_seasons_list()

    drop_task = drop_old_data()
    seasons_data = fetch_seasons()

    drop_task >> seasons_data

    TriggerDagRunOperator.partial(
        task_id="trigger_all_season_results",
        trigger_dag_id="biathlon_season_results",
        wait_for_completion=True,
        max_active_tis_per_dag=1,
    ).expand(conf=seasons_data)


biathlon_update_dag()
