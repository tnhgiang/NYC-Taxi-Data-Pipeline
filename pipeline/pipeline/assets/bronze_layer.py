from datetime import datetime, timedelta

import polars as pl
from dagster import AssetExecutionContext, Output, asset

from ..partitions import daily_partitions


@asset(
    name="bronze_yellow_taxi_trips",
    key_prefix=["bronze", "nyc_taxi"],
    group_name="bronze",
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="parquet_io_manager",
    compute_kind="MySQL",
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

    return Output(
        pl_data,
        metadata={
            "table": "yellow_taxi_trips",
            "row_count": len(pl_data),
            "column_names": pl_data.columns,
        },
    )


@asset(
    name="bronze_taxi_zone",
    key_prefix=["bronze", "nyc_taxi"],
    group_name="bronze",
    required_resource_keys={"csv_downloader"},
    io_manager_key="csv_io_manager",
    compute_kind="Python",
)
def bronze_taxi_zone(context: AssetExecutionContext) -> Output[str]:
    csv_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    tmp_file_path = context.resources.csv_downloader.download(context, csv_url)
    context.log.info(f"Downloaded taxi zone lookup data to {tmp_file_path}")

    return Output(
        tmp_file_path,
        metadata={
            "table": "taxi_zones",
        },
    )


@asset(
    name="bronze_taxi_zone_geometry",
    key_prefix=["bronze", "nyc_taxi"],
    group_name="bronze",
    required_resource_keys={"zipfile_downloader"},
    io_manager_key="shapefile_io_manager",
    compute_kind="Python",
)
def bronze_taxi_zone_geometry(context: AssetExecutionContext) -> Output[str]:
    shapefile_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

    tmp_folder_path = context.resources.zipfile_downloader.download(
        context, shapefile_url
    )
    context.log.info(f"Downloaded taxi zone geometry data to {tmp_folder_path}")

    return Output(
        tmp_folder_path,
        metadata={
            "table": "taxi_zones",
        },
    )
