from datetime import datetime
from logging import getLogger
from os import listdir
from pathlib import Path

from airflow.sdk import DAG, task
from pandas import DataFrame, read_csv

from choices.name_tables import TableNames
from functions.insert_values_to_database import load_to_database
from sdk.clickhouse_sdk import TableDropper


log = getLogger(__name__)


DATA_MAPPING = [
    {"file": "olympic_events.csv", "table": TableNames.BIATHLON_OG_EVENTS.value},
    {"file": "olympic_competitions.csv", "table": TableNames.BIATHLON_OG_COMPETITION.value},
    {"file": "olympic_results.csv", "table": TableNames.BIATHLON_OG_RESULT.value},
]


with DAG(
    dag_id="biathlon_update_old_olympic_games",
    description="Обновление данных по олимпийским играм с 1960-1980",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["biathlon", "manual"],
    max_active_tasks=5,
) as dag:

    @task()
    def get_data(file_name: str) -> DataFrame:

        base_path = Path(__file__).parent
        file_path = base_path / "file_db" / file_name

        if not file_path.exists():
            log.error(f"Содержимое папки {base_path / 'file_db'}: {listdir(base_path / 'file_db')}")
            raise FileNotFoundError(f"Файл не найден по пути {file_path}")

        events = read_csv(file_path, index_col=False)
        log.debug(f"Events: {events}")

        return events

    @task()
    def load_data_to_clickhouse(events_df: DataFrame, table_name: str) -> list[int]:
        TableDropper().drop_tables([table_name])
        load_to_database(data=events_df, table_name=table_name)

    for item in DATA_MAPPING:
        file_name = item["file"]
        table_name = item["table"]
        raw_data = get_data.override(task_id=f"get_data_from_{file_name}")(file_name=file_name)
        load_data_to_clickhouse.override(task_id=f"load_data_to_{table_name}")(
            events_df=raw_data, table_name=table_name
        )
