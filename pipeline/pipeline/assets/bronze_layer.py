from datetime import datetime, timedelta

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from ..partitions import daily_partitions


@asset(
    name="bronze_yellow_taxi_trips",
    io_manager_key="parquet_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "nyc_taxi"],
    compute_kind="MySQL",
    group_name="bronze",
    partitions_def=daily_partitions,
)
def bronze_yellow_taxi_trips(context: AssetExecutionContext) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM yellow_taxi_trips"

    partition_date_str = context.partition_key

    partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    next_date_str = (partition_date + timedelta(days=1)).strftime("%Y-%m-%d")
    if partition_date_str:
        sql_stm += (
            f" WHERE tpep_pickup_datetime >= '{partition_date_str}'"
            f" and tpep_pickup_datetime < '{next_date_str}'"
        )
        context.log.info(f"Loading data for partition date: {partition_date_str}")
    else:
        context.log.info("No partition date provided. Full loading data.")

    pl_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(pl_data)
