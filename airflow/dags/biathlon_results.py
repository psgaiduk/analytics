from datetime import datetime
from logging import getLogger

from airflow.sdk import DAG, Param, task

from constants import BIATHLON_RESULTS_URL
from sdk.clickhouse_sdk import InsertDataFrame, GetDataByQuery


log = getLogger(__name__)

with DAG(
    dag_id="biathlon_results",
    schedule="0 */3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon"],
    params={
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
    def get_race_id() -> int:
        query_for_get_race_id = """
        SELECT
            RaceId
        FROM biathlon.competitions 
        WHERE RaceId NOT IN (SELECT DISTINCT race_id FROM biathlon.results)
        AND StatusId = 11
        ORDER BY StartTime DESC
        LIMIT 1
        """

        race_id_df = GetDataByQuery().get_data(query=query_for_get_race_id)
        log.info(f"Get race id from database race_id_df = {race_id_df}")
        if race_id_df.empty:
            log.warning("Race Id is empty")

        race_id = race_id_df["RaceId"].iloc[0]
        return race_id

    @task()
    def fetch_results(race_id: int, **kwargs) -> list:
        """Загружает данные соревнований с biathlonresults.com"""
        from time import sleep

        from pandas import DataFrame, concat
        from requests import get

        results = DataFrame()
        analytic_results = DataFrame()
        rt = kwargs["params"]["rt"]

        url = f"{BIATHLON_RESULTS_URL}/Results?RT={rt}&RaceId={race_id}"
        response = get(url, timeout=30)
        log.info(f"Status code for race_id {race_id}: {response.status_code}")
        if response.status_code != 200:
            log.error(f"Get error response: {response.text}")
            return

        data = response.json()
        if not data:
            return

        df = DataFrame(data["Results"])
        df["race_id"] = race_id
        df["rt"] = rt
        results = concat([results, df], ignore_index=True)
        sleep(1)

        competition = data["Competition"]
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

        for type_id, type_name in analytic_types:
            analytics_url = f"{BIATHLON_RESULTS_URL}/AnalyticResults?RaceId={race_id}&TypeId={type_id}"
            response = get(analytics_url, timeout=30)
            log.info(f"Status code for type_id {type_id}: {response.status_code}")
            if response.status_code != 200:
                log.error(f"Get error response: {response.text}")
                continue
            analytics_data = response.json()
            if not analytics_data:
                continue

            df = DataFrame(analytics_data["Results"])
            df["race_id"] = race_id
            df["type_id"] = type_id
            df["type_name"] = type_name
            analytic_results = concat([analytic_results, df], ignore_index=True)

            log.info(f"data: {df.info()}")
            log.info(f"data: {df.head()}")
            sleep(1)

        return [results, analytic_results]

    @task
    def load_to_clickhouse(all_results: list, **kwargs) -> None:
        """
        Сохраняет данные в ClickHouse
        """

        results, analytics_results = all_results
        recreate = kwargs["params"]["recreate"]
        log.info(f"Start load data to clickhouse, recreate={recreate}, {type(recreate)}")

        if results.empty:
            log.error("DataFrame пуст")
        else:
            table_name = "biathlon.results"
            InsertDataFrame(df=results, table_name=table_name).insert_data(recreate=recreate)

        if analytics_results.empty:
            log.error("Analytics DataFrame пуст")
        else:
            table_name = "biathlon.analytics_results"
            InsertDataFrame(df=analytics_results, table_name=table_name).insert_data(recreate=recreate)

    race_id = get_race_id()
    all_results = fetch_results(race_id=race_id)
    load_to_clickhouse(all_results=all_results)
