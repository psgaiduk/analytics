from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, task
from airflow.models.param import Param

from constants import BIATHLON_RESULTS_URL
from sdk.clickhouse_sdk import DeleteFromDatabase, InsertDataFrame


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_competitions",
    schedule="0 13 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon"],
    params={
        "season_id": Param(
            0,
            type="integer",
            description="SeasonId для biathlonresults.com",
        ),
        "rt": Param(
            385698,
            type="integer",
            description="RT для biathlonresults.com",
        ),
        "recreate": Param(
            False,
            type="boolean",
            description="Recreate table in database or not",
        ),
    },
) as dag:

    @task()
    def fetch_competitions(**kwargs) -> list:
        """Загружает данные соревнований с biathlonresults.com"""
        from time import sleep

        from pandas import DataFrame, concat
        from requests import get

        results = DataFrame()

        rt = kwargs["params"]["rt"]
        season_id = kwargs["params"]["season_id"] or generate_season_id()
        log.info(f"Get values rt = {rt} and season_id = {season_id}")

        stages = ["CH__", "OG__"]
        stages.extend([f"CP{i}" if i > 9 else f"CP0{i}" for i in range(1, 20)])
        log.info(f"stages: {stages}")

        for stage in stages:
            event_id = f"BT{season_id}SWRL{stage}"
            url = f"{BIATHLON_RESULTS_URL}/Competitions?RT={rt}&EventId={event_id}"
            response = get(url, timeout=30)

            log.info(f"Status code for event_id {event_id}: {response.status_code}")
            if response.status_code != 200:
                log.error(f"Get error response: {response.text}")
                break

            data = response.json()
            if not data:
                continue

            df = DataFrame(data)
            df["season_id"] = season_id
            df["stage"] = stage
            df["rt"] = rt

            results = concat([results, df], ignore_index=True)

            sleep(1)

        log.info(f"data: {df.info()}")
        log.info(f"data: {df.head()}")
        return results

    def generate_season_id() -> str:
        """
        Генерирует season_id по текущей дате.

        Returns:
            str: season_id
        """
        from datetime import date

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

    @task
    def load_to_clickhouse(competitions: list, **kwargs) -> None:
        """
        Сохраняет данные в ClickHouse
        """

        recreate = kwargs["params"]["recreate"]
        log.info(f"Start load data to clickhouse, recreate={recreate}, {type(recreate)}")

        if competitions.empty:
            print("DataFrame пуст")
            return

        season_id = kwargs["params"]["season_id"] or generate_season_id()
        table_name = "biathlon_raw.competition"
        DeleteFromDatabase(table_name=table_name).delete_where(condition=f"season_id = '{season_id}'")
        InsertDataFrame(df=competitions, table_name=table_name).insert_data(recreate=recreate)

    competitions = fetch_competitions(
        season_id=dag.params["season_id"],
        rt=dag.params["rt"],
    )

    load_to_clickhouse(
        competitions=competitions,
        recreate=dag.params["recreate"],
    )
