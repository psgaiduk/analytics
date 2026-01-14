from datetime import date, datetime
from logging import getLogger
from time import sleep

from airflow.sdk import DAG, Param, task
from clickhouse_connect.driver.exceptions import DatabaseError

from choices.name_tables import TableNames
from functions.insert_values_to_database import load_to_database
from sdk.biathlon.fetch_data import BiathlonCompetitionsFetcher, BiathlonResultsFetcher
from sdk.clickhouse_sdk import GetDataByQuery, DeleteFromDatabase


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
) as dag:

    @task()
    def get_competitions(**kwargs):
        rt = kwargs["params"]["rt"]
        season_id = kwargs["params"]["season_id"] or generate_season_id()
        table_name = TableNames.BIATHLON_COMPETITION.value

        DeleteFromDatabase(table_name=table_name).delete_where(condition=f"season_id = '{season_id}'")
        competitions = BiathlonCompetitionsFetcher().fetch(rt=rt, season_id=season_id)
        load_to_database(data=competitions, table_name=table_name)

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

    @task()
    def get_results(**kwargs):
        rt = kwargs["params"]["rt"]
        season_id = kwargs["params"]["season_id"] or generate_season_id()

        table_results = TableNames.BIATHLON_RESULT.value
        table_analytics_results = TableNames.BIATHLON_ANALYTICS_RESULT.value
        DeleteFromDatabase(table_name=table_results).delete_where(condition=f"season_id = '{season_id}'")
        DeleteFromDatabase(table_name=table_analytics_results).delete_where(condition=f"season_id = '{season_id}'")

        while True:
            race_id = _get_race_id(season_id=season_id)
            if not race_id:
                log.info("Not races without results for season stop working.")
                break
            log.info(f"start update race id = {race_id}")
            results, analytics_results = BiathlonResultsFetcher().fetch(season_id=season_id, race_id=race_id, rt=rt)
            load_to_database(table_name=table_results, data=results)
            load_to_database(table_name=table_analytics_results, data=analytics_results)
            log.info(f"complete update race id = {race_id}")
            sleep(5)

    def _get_race_id(season_id: str) -> int:
        log.info(f"Get season_id = {season_id}")
        query_for_get_race_id = f"""
        SELECT
            RaceId
        FROM {TableNames.BIATHLON_COMPETITION.value}
        WHERE StatusId = '11' AND season_id = '{season_id}'
        AND RaceId NOT IN (
            SELECT
                DISTINCT race_id
            FROM {TableNames.BIATHLON_RESULT.value}
            WHERE season_id = '{season_id}'
        )
        ORDER BY StartTime DESC
        LIMIT 1
        """
        try:
            race_id_df = GetDataByQuery().get_data(query=query_for_get_race_id)
        except DatabaseError as e:
            log.error(f"Database error {e}")
            query_for_get_race_id = f"""
                SELECT
                    RaceId
                FROM {TableNames.BIATHLON_COMPETITION.value}
                WHERE StatusId = '11' AND season_id = '{season_id}'
                ORDER BY StartTime DESC
                LIMIT 1
                """
            race_id_df = GetDataByQuery().get_data(query=query_for_get_race_id)

        log.info(f"Get race id from database race_id_df = {race_id_df}")
        if race_id_df.empty:
            log.warning("Race Id is empty")
            return

        return race_id_df["RaceId"].iloc[0]

    get_competitions_task = get_competitions()
    get_results_task = get_results()

    get_competitions_task >> get_results_task
