# dags/sdk/clickhouse_sdk.py

from os import getenv
from typing import Iterable, Tuple, Dict

import pandas as pd
from pandas import DataFrame
from clickhouse_connect import get_client


__all__ = [
    "DatabaseClient",
    "CreateTable",
    "InsertDataFrame",
]


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
    """
    Вставка pandas.DataFrame в ClickHouse
    с автоматическим определением схемы.
    """

    DEFAULT_TYPE_MAP: Dict[str, str] = {
        "object": "String",
        "int64": "Int64",
        "float64": "Float64",
        "bool": "UInt8",
        "datetime64[ns]": "DateTime",
    }

    def __init__(self, df: DataFrame, table_name: str) -> None:
        super().__init__()
        self.df = df
        self.table_name = table_name

    def infer_ch_schema(self) -> list[Tuple[str, str]]:
        """
        Определяет схему ClickHouse по dtypes DataFrame.
        """
        columns: list[Tuple[str, str]] = []

        for col, dtype in self.df.dtypes.items():
            ch_type = self.DEFAULT_TYPE_MAP.get(str(dtype), "String")
            columns.append((col, ch_type))

        return columns

    def insert_data(self, recreate: bool = False) -> int:
        """
        Создаёт таблицу (если нужно) и вставляет данные.

        :param recreate: если True — таблица будет пересоздана
        :return: количество вставленных строк
        """
        creator = CreateTable(self.table_name)

        if recreate:
            self.client.command(f"DROP TABLE IF EXISTS {self.table_name}")

        columns = self.infer_ch_schema()
        creator.create_table(columns=columns)

        # NaN -> None (важно для Nullable)
        df_clean = self.df.where(pd.notnull(self.df), None)

        self.client.insert_df(self.table_name, df_clean)

        return len(df_clean)
