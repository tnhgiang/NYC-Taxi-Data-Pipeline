from contextlib import contextmanager

import polars as pl
from dagster import InputContext, IOManager, OutputContext
from sqlalchemy import create_engine


@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise


class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        # Extract data from MySQL
        with connect_mysql(self._config) as db_conn:
            pl_data = pl.read_database(sql, db_conn)
            return pl_data
