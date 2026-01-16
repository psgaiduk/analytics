# dags/sdk/clickhouse_sdk.py

from logging import getLogger
from os import getenv
from typing import Iterable, Tuple, List
from urllib3.exceptions import NewConnectionError, MaxRetryError, HTTPError

from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError, DatabaseError
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


__all__ = [
    "DatabaseClient",
    "CreateTable",
    "InsertDataFrame",
]

log = getLogger(__name__)


RETRYABLE_ERRORS = (OperationalError, DatabaseError, NewConnectionError, MaxRetryError, HTTPError)


class DatabaseClient:
    """
    Базовый клиент ClickHouse.
    Отвечает только за создание подключения.
    """

    def __init__(self) -> None:
        self.client = self._get_client()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(f"Retrying create client (attempt {retry_state.attempt_number})..."),
        reraise=True,
    )
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
            connect_timeout=30,
        )


class CreateTable(DatabaseClient):
    """
    Логика создания таблицы в ClickHouse.
    """

    def __init__(self, table_name: str) -> None:
        super().__init__()
        self.table_name = table_name

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(f"Retrying create table (attempt {retry_state.attempt_number})..."),
        reraise=True,
    )
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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(f"Retrying insert data (attempt {retry_state.attempt_number})..."),
        reraise=True,
    )
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

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying get data by query (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
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


class DeleteFromDatabase(DatabaseClient):
    """Удаляет данные из базы данных."""

    def __init__(self, table_name: str) -> None:
        super().__init__()
        self.table_name = table_name

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(
            f"Retrying delete from database (attempt {retry_state.attempt_number})..."
        ),
        reraise=True,
    )
    def delete_where(self, condition: str) -> None:
        """
        Удаляет данные по условию.

        Args:
            condition: SQL-условие для WHERE
        """
        check_sql = f"EXISTS TABLE {self.table_name}"
        table_exists = self.client.command(check_sql)

        if table_exists:
            sql = f"""
            ALTER TABLE {self.table_name}
            DELETE WHERE {condition}
            """
            log.info(f"Deleting from {self.table_name} where {condition}")
            self.client.command(sql)
        else:
            log.warning(f"Table {self.table_name} does not exist. Skipping delete.")


class TableDropper(DatabaseClient):
    """Удаление таблиц из базы данных."""

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        before_sleep=lambda retry_state: log.warning(f"Retrying drop table (attempt {retry_state.attempt_number})..."),
        reraise=True,
    )
    def drop_tables(self, tables: list[str]) -> None:
        """Полное удаление указанных таблиц."""
        self._drop_need_tables(tables=tables)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type(RETRYABLE_ERRORS),
        reraise=True,
    )
    def drop_all_tables_in_db(self, db_name: str) -> None:
        """Удаляет все таблицы из указанной базы данных, не удаляя саму базу."""

        tables_query = f"SELECT name FROM system.tables WHERE database = '{db_name}'"

        result = self.client.query(tables_query)
        tables = [row[0] for row in result.result_rows]

        if not tables:
            log.info(f"No tables found in database: {db_name}")
            return

        log.info(f"Found {len(tables)} tables in {db_name}. Starting deletion...")

        self._drop_need_tables(tables=tables)

        log.info(f"All tables in {db_name} have been dropped.")

    def _drop_need_tables(self, tables: str) -> None:
        """Полное удаление указанных таблиц."""
        log.info(f"Dropping tables: {tables}")
        for table in tables:
            log.info(f"Dropping table: {table}")
            self.client.command(f"DROP TABLE IF EXISTS {table}")
