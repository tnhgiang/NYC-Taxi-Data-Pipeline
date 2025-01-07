from contextlib import contextmanager
from typing import Any, Union

import polars as pl
from clickhouse_driver import Client
from dagster import InputContext, IOManager, OutputContext
from pyspark.sql import DataFrame

from ..utils import get_current_time


@contextmanager
def connect_clickhouse(config):
    client = Client.from_url(
        f"clickhouse://{config.get('user')}:{config.get('password')}@"
        f"{config.get('host')}:{config.get('port')}/{config.get('database')}"
    )
    try:
        yield client
    except Exception:
        raise


class ClickHouseIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    @staticmethod
    def _insert_data_into_clickhouse(
        table_name: str,
        column_names: list,
        client: Any,
        obj: Union[pl.DataFrame, DataFrame],
    ):
        """Insert data to ClickHouse."""
        if isinstance(obj, pl.DataFrame):
            df = obj[column_names] if len(column_names) > 0 else obj
            client.insert_dataframe(
                f"INSERT INTO {table_name} VALUES",
                df.to_pandas(),
                settings=dict(use_numpy=True),
            )
        else:
            df = obj.select(*column_names) if len(column_names) > 0 else obj
            df.writeTo(f"clickhouse.{table_name}").append()

    def handle_output(
        self, context: OutputContext, obj: Union[pl.DataFrame, DataFrame]
    ):
        schema, table = context.asset_key.path[1:]
        tmp_table = f"{schema}.tmp_{table}_{get_current_time(only_time=True)}"
        target_table = f"{schema}.{table}"

        # Get table schema information
        primary_keys = (context.metadata or {}).get("primary_keys", [])
        column_names = (context.metadata or {}).get("columns", [])

        try:
            with connect_clickhouse(self._config) as client:
                # Create a temporary table in ClickHouse
                sql_stm = (
                    f"CREATE TABLE IF NOT EXISTS {tmp_table} AS {target_table} "
                    f"ENGINE MergeTree() ORDER BY ({','.join(primary_keys)});"
                )
                client.execute(sql_stm)
                self._insert_data_into_clickhouse(tmp_table, column_names, client, obj)
                context.log.debug(
                    f"{self.__class__.__name__}: {tmp_table} created in ClickHouse"
                )

                if len(primary_keys) > 0:
                    context.log.info(
                        f"{self.__class__.__name__}: Delete duplicate row with "
                        f"primary keys: {primary_keys}"
                    )

                    # Conditions for removing duplicate rows
                    conditions = " AND ".join(
                        [
                            f"{pk} IN (SELECT {pk} FROM {tmp_table})"
                            for pk in primary_keys
                        ]
                    )

                    # Delete duplicate rows
                    client.execute(
                        f"ALTER TABLE {target_table} DELETE WHERE {conditions};"
                    )
                    # Insert new rows
                    client.execute(
                        f"INSERT INTO {target_table} SELECT * FROM {tmp_table};"
                    )
                else:
                    context.log.info(
                        f"{self.__class__.__name__}: No primary keys found. "
                        f"Truncate the {target_table} table."
                    )

                    # Truncate the table
                    client.execute(f"TRUNCATE TABLE {target_table};")
                    # Insert new rows
                    client.execute(
                        f"INSERT INTO {target_table} SELECT * FROM {tmp_table};"
                    )

                context.log.info(
                    f"{self.__class__.__name__}: Rows inserted successfully"
                )

                # Drop the temporary table
                client.execute(f"DROP TABLE IF EXISTS {tmp_table}")
                context.log.debug(f"{self.__class__.__name__}: {tmp_table} dropped")
        except Exception as e:
            context.log.error(
                f"{self.__class__.__name__}: Failed to handle output due to {str(e)}"
            )
            raise

    def load_input(self, context: InputContext):
        pass
