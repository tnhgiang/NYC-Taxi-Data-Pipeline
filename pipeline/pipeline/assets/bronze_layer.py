from datetime import datetime

from dagster import AssetExecutionContext, Output, asset

from ..partitions import monthly_partitions


@asset(
    name="bronze_yellow_taxi_trips",
    key_prefix=["bronze", "nyc_taxi"],
    group_name="bronze",
    required_resource_keys={"parquet_downloader"},
    io_manager_key="parquet_io_manager",
    compute_kind="Python",
    partitions_def=monthly_partitions,
)
def bronze_yellow_taxi_trips(context: AssetExecutionContext) -> Output[str]:
    partition_month_str = context.partition_key

    if not partition_month_str:
        context.log.error("No partition key provided")
        raise ValueError("No partition key provided")

    partition_month_str = datetime.strptime(
        partition_month_str, "%Y-%m-%d"
    ).strftime("%Y-%m")
    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{partition_month_str}.parquet"
    )

    tmp_file_path = context.resources.parquet_downloader.download(context, url)
    context.log.info(f"Ingested data from {url}")

    return Output(
        tmp_file_path,
        metadata={
            "url": url,
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
