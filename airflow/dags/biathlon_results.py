from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, Param, task
from pandas import DataFrame, concat

from choices.name_tables import TableNames
from functions.insert_values_to_database import load_to_database
from sdk.biathlon.fetch_data import BiathlonAnalyticsResultsFetcher, BiathlonResultsFetcher
from sdk.clickhouse_sdk import DeleteFromDatabase


log = getLogger(__name__)


with DAG(
    dag_id="biathlon_update_race_results",
    description="Обновление результатов одной гонки с biathlonresults.com",
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
    max_active_tasks=5,
) as dag:

    @task()
    def get_race_results(**kwargs) -> tuple[DataFrame, dict]:
        """Get race results from biathlonresults.com.

        Returns:
            tuple[DataFrame, dict]: DataFrame with results for race, info about this race.
        """
        rt = kwargs["params"]["rt"]
        race_id = kwargs["params"]["race_id"]
        return BiathlonResultsFetcher(rt=rt).fetch(race_id=race_id)

    @task()
    def load_race_results(results_and_competition: tuple[DataFrame, dict], **kwargs) -> dict:
        """Load race results to clickhouse.

        Args:
            results_and_competition (tuple[DataFrame, dict]): DataFrame with results for race, info about this race.

        Returns:
            dict: info about this race.
        """
        race_id = kwargs["params"]["race_id"]
        results, competition = results_and_competition

        table_results = TableNames.BIATHLON_RESULT.value
        DeleteFromDatabase(table_name=table_results).delete_where(condition=f"race_id = '{race_id}'")
        load_to_database(table_name=table_results, data=results)
        return competition

    @task()
    def get_analytics_type(competition: dict) -> list[list[str]]:
        """Create analytics type for this race.

        Args:
            competition (dict): info about this race.

        Returns:
            list[list[str]]: List with analytics types (id of this type and pretty name of this type analytics).
        """
        legs = int(competition.get("NrLegs", 0))
        shootings = int(competition.get("NrShootings", 0))
        analytic_types = [
            ["CRST", "Total Course Time"],
            ["RNGT", "Total Range Time"],
            ["STTM", "Total Shooting Time"],
            ["SKIT", "Ski Time"],
        ]

        if legs:
            analytic_types.extend([[f"FI{i + 1}L", f"Results Les {i + 1}"] for i in range(legs)])
            analytic_types.extend([[f"CRST{i + 1}", f"Course Time Leg {i + 1}"] for i in range(legs)])
            analytic_types.extend([[f"RNGT{i + 1}T", f"Range Time Leg {i + 1}"] for i in range(legs)])
        else:
            legs = 1

        analytic_types.extend([[f"CRS{i + 1}", f"Course Time Lap {i + 1}"] for i in range((shootings + 1) * legs)])
        analytic_types.extend([[f"RNG{i + 1}", f"Range Time {i + 1}"] for i in range(shootings * legs)])
        analytic_types.extend([[f"S{i + 1}TM", f"Shooting Time {i + 1}"] for i in range(shootings * legs)])
        log.info(f"analytic_types: {analytic_types}")
        return analytic_types

    @task()
    def get_race_analytics_results(analytic_type: list[str], **kwargs) -> DataFrame:
        """Get analytics results for analytic_type of this race.

        Args:
            analytic_type (list[str]): list with id of this type and pretty name of this type.

        Returns:
            DataFrame: Analytics results for this analytics type of this race.
        """
        race_id = kwargs["params"]["race_id"]
        rt = kwargs["params"]["rt"]
        type_id, type_name = analytic_type

        return BiathlonAnalyticsResultsFetcher(rt=rt, race_id=race_id).fetch(type_name=type_name, type_id=type_id)

    @task()
    def merge_and_load_analytics_result(dfs_list: list[DataFrame], **kwargs) -> None:
        """Merge all analytics results and load it to clickhouse.

        Args:
            dfs_list (list[DataFrame]): List with analytics results dataframes.
        """
        race_id = kwargs["params"]["race_id"]
        competitions_df = concat(dfs_list, ignore_index=True)
        table_name = TableNames.BIATHLON_ANALYTICS_RESULT.value
        DeleteFromDatabase(table_name=table_name).delete_where(condition=f"race_id = '{race_id}'")
        load_to_database(data=competitions_df, table_name=table_name)

    results_and_competition = get_race_results()
    competition = load_race_results(results_and_competition=results_and_competition)
    analytic_types = get_analytics_type(competition=competition)
    dfs_list = get_race_analytics_results.partial(max_active_tis_per_dag=1).expand(analytic_type=analytic_types)
    merge_and_load_analytics_result(dfs_list=dfs_list)
