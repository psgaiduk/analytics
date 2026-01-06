# dags/sdk/clickhouse_sdk.py

from logging import getLogger
from os import getenv
from typing import Iterable, Tuple, List

from clickhouse_connect import get_client
import pandas as pd


__all__ = [
    "DatabaseClient",
    "CreateTable",
    "InsertDataFrame",
]

log = getLogger(__name__)


class DatabaseClient:
    """
    Базовый клиент ClickHouse.
    Отвечает только за создание подключения.
    """

    def __init__(self) -> None:
        self.client = self._get_client()

    def _get_client(self):
        """
        Создаёт и возвращает ClickHouse client
        на основе переменных окружения.
        """
        return get_client(
            host=getenv("CLICKHOUSE_HOST"),
            port=int(getenv("CLICKHOUSE_PORT", "8123")),
            username=getenv("CLICKHOUSE_USER"),
            password=getenv("CLICKHOUSE_PASSWORD"),
            database=getenv("CLICKHOUSE_DB"),
        )


class CreateTable(DatabaseClient):
    """
    Логика создания таблицы в ClickHouse.
    """

    def __init__(self, table_name: str) -> None:
        super().__init__()
        self.table_name = table_name

    def create_table(
        self,
        columns: Iterable[Tuple[str, str]],
        engine: str = "MergeTree",
        order_by: str = "tuple()",
    ) -> None:
        """
        Создаёт таблицу, если она не существует.

        :param columns: iterable из (column_name, clickhouse_type)
        :param engine: ClickHouse engine
        :param order_by: ORDER BY выражение
        """
        columns_ddl = ", ".join(f"`{c}` Nullable({t})" for c, t in columns)

        self.client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                {columns_ddl}
            )
            ENGINE = {engine}
            ORDER BY {order_by}
            """
        )


class InsertDataFrame(DatabaseClient):
    """Вставляет данные из DataFrame в таблицу ClickHouse."""

    def __init__(self, df: pd.DataFrame, table_name: str) -> None:
        super().__init__()
        self.df = df
        self.table_name = table_name
        self.schema_inferencer = ClickHouseSchemaInferencer()

    def insert_data(self, recreate: bool = False) -> int:
        """
        Вставляет данные из DataFrame в таблицу ClickHouse.

        Args:
            recreate (bool, optional): Если True, то таблица будет пересоздана перед вставкой данных. Defaults to False.

        Returns:
            int: Количество вставленных строк.
        """
        creator = CreateTable(self.table_name)

        if recreate:
            log.info(f"Recreating table {self.table_name}")
            self.client.command(f"DROP TABLE IF EXISTS {self.table_name}")

        df_clean, columns = self.schema_inferencer.infer_and_cast(self.df)

        creator.create_table(columns=columns)

        self.client.insert_df(self.table_name, df_clean)

        return len(df_clean)


class ClickHouseSchemaInferencer:
    """Всегда строка"""

    def infer_and_cast(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Tuple[str, str]]]:
        """
        Приводит колонки DataFrame к нужным типам и возвращает схему.

        Args:
            df (pd.DataFrame): dataframe для приведения колонок к нужному типу данных.

        Returns:
            Tuple[pd.DataFrame, List[Tuple[str, str]]]: приведённый dataframe и схема.
        """
        df_casted = df.copy()
        schema: List[Tuple[str, str]] = []

        for column in df_casted.columns:
            series = df_casted[column]
            log.info(f"Column '{column}' inferred as String")
            df_casted[column] = series.astype(str)
            schema.append((column, "String"))

        log.info(f"schema = {schema}")
        return df_casted, schema


class GetDataByQuery(DatabaseClient):
    """
    Выполнение SELECT-запросов к ClickHouse с возвратом результата в pandas.DataFrame.
    """

    def get_data(self, query: str, parameters: dict | None = None) -> pd.DataFrame:
        """
        Выполняет SELECT-запрос и возвращает результат как DataFrame.

        Args:
            query (str): SQL SELECT запрос
            parameters (dict | None): параметры запроса (optional)

        Returns:
            pd.DataFrame: результат запроса

        Example:
        query =
            SELECT *
            FROM biathlon_raw.result
            WHERE season_id = %(season_id)s
        df = GetDataByQuery().get_data(
            query,
            parameters={"season_id": 2526},
        )
        """
        log.info("Executing ClickHouse query")
        log.debug("SQL: %s", query)
        log.debug("Parameters: %s", parameters)

        result = self.client.query(
            query,
            parameters=parameters,
        )

        df = pd.DataFrame(
            result.result_rows,
            columns=result.column_names,
        )

        log.info("Query returned %s rows", len(df))
        return df
