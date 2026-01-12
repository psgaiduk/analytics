from logging import getLogger

from pandas import DataFrame

from sdk.clickhouse_sdk import InsertDataFrame


log = getLogger(__name__)


def load_to_database(table_name: str, data: DataFrame, recreate: bool) -> None:
    """Сохраняет данные в базу данных.

    Args:
        table_name (str): name of table.
        data (DataFrame): data for load in table.
        recreate (bool): recreate table or not (This delete all data).
    """

    log.info(f"Start load data to database, recreate={recreate}, {type(recreate)}")

    if data.empty:
        print("DataFrame пуст")
        return

    InsertDataFrame(df=data, table_name=table_name).insert_data(recreate=recreate)
