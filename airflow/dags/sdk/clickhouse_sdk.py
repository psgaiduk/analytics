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
    """
    Приведение pandas.DataFrame к совместимым типам.

    Принципы:
    - DateTime → datetime или ISO-строки
    - Int64    → целые числа без дробной части
    - Float64  → любые числовые значения с дробями
    - UInt8    → bool-подобные значения
    - String   → fallback
    """

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
            series = series.dropna().astype(str).str.strip()

            series = series[series != ""]
            log.info(f"Inferring type for column '{column}' with sample data: {series.head(5).tolist()}")

            if series.empty:
                log.info(f"Column '{column}' is empty after cleaning, defaulting to String")
                schema.append((column, "String"))

            elif self._is_bool(series):
                log.info(f"Column '{column}' inferred as Boolean")
                df_casted[column] = self._cast_bool(series)
                schema.append((column, "UInt8"))

            elif self._is_int(series):
                log.info(f"Column '{column}' inferred as Integer")
                df_casted[column] = pd.to_numeric(series, errors="coerce").astype("Int64")
                schema.append((column, "Int64"))

            elif self._is_float(series):
                log.info(f"Column '{column}' inferred as Float")
                df_casted[column] = pd.to_numeric(series, errors="coerce").astype("Float64")
                schema.append((column, "Float64"))

            elif self._is_datetime(series):
                log.info(f"Column '{column}' inferred as DateTime")
                df_casted[column] = pd.to_datetime(series, errors="coerce")
                schema.append((column, "DateTime"))

            else:
                log.info(f"Column '{column}' inferred as String")
                df_casted[column] = series.astype(str)
                schema.append((column, "String"))

        return df_casted, schema

    # ---------- type checks ----------

    @staticmethod
    def _is_datetime(series: pd.Series) -> bool:
        if pd.api.types.is_numeric_dtype(series):
            return False

        try:
            pd.to_datetime(series, errors="raise", utc=True)
            return True
        except Exception:
            return False

    @staticmethod
    def _is_int(series: pd.Series) -> bool:
        try:
            numeric = pd.to_numeric(series.dropna(), errors="raise")
            return (numeric % 1 == 0).all()
        except Exception:
            return False

    @staticmethod
    def _is_float(series: pd.Series) -> bool:
        try:
            numeric = pd.to_numeric(series.dropna(), errors="raise")
            return not (numeric % 1 == 0).all()
        except Exception:
            return False

    @staticmethod
    def _is_bool(series: pd.Series) -> bool:
        non_null = series.dropna().astype(str).str.lower()
        return set(non_null.unique()).issubset({"true", "false", "1", "0"})

    # ---------- casting helpers ----------

    @staticmethod
    def _cast_bool(series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.lower()
            .map(
                {
                    "true": 1,
                    "false": 0,
                    "1": 1,
                    "0": 0,
                }
            )
            .astype("Int8")
        )
