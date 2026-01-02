from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, task
from airflow.models.param import Param

from constants import BIATHLON_RESULTS_URL
from sdk.clickhouse_sdk import InsertDataFrame


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_competitions",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon"],
    params={
        "season_id": Param(
            2526,
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
    def fetch_competitions(rt: int, season_id: int) -> list:
        """Загружает данные соревнований с biathlonresults.com"""
        from time import sleep

        from pandas import DataFrame, concat
        from requests import get

        results = DataFrame()

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

    @task
    def load_to_clickhouse(competitions: list, recreate: bool) -> None:
        """
        Сохраняет данные в ClickHouse
        """

        log.info(f"Start load data to clickhouse, recreate={recreate}, {type(recreate)}")

        if competitions.empty:
            print("DataFrame пуст")
            return

        table_name = "biathlon.competitions"

        InsertDataFrame(df=competitions, table_name=table_name).insert_data(recreate=recreate)

    competitions = fetch_competitions(
        season_id=dag.params["season_id"],
        rt=dag.params["rt"],
    )

    load_to_clickhouse(
        competitions=competitions,
        recreate=dag.params["recreate"],
    )
