import polars as pl
from dagster import AssetExecutionContext, AssetIn, Output, asset
from pyspark.sql import DataFrame

from ..partitions import daily_partitions


@asset(
    ins={
        "silver_cleaned_taxi_zone": AssetIn(key_prefix=["silver", "nyc_taxi"]),
        "silver_cleaned_taxi_zone_geometry": AssetIn(key_prefix=["silver", "nyc_taxi"]),
    },
    name="dim_locations",
    key_prefix=["gold", "nyc_taxi"],
    group_name="gold",
    io_manager_key="parquet_io_manager",
    compute_kind="Polars",
)
def dim_locations(
    context: AssetExecutionContext,
    silver_cleaned_taxi_zone: pl.DataFrame,
    silver_cleaned_taxi_zone_geometry: pl.DataFrame,
) -> Output[pl.DataFrame]:
    """
    Create the Location Dimension Table.
    """
    df = silver_cleaned_taxi_zone.join(
        silver_cleaned_taxi_zone_geometry, on=["location_id", "borough", "zone"]
    )

    return Output(df, metadata={"row_count": df.height, "column_names": df.columns})


@asset(
    ins={
        "silver_cleaned_yellow_taxi_trips": AssetIn(key_prefix=["silver", "nyc_taxi"]),
    },
    name="fact_trips",
    key_prefix=["gold", "nyc_taxi"],
    group_name="gold",
    io_manager_key="spark_io_manager",
    compute_kind="Spark",
    partitions_def=daily_partitions,
)
def fact_trips(
    context: AssetExecutionContext, silver_cleaned_yellow_taxi_trips: DataFrame
) -> Output[DataFrame]:
    """
    Create the Fact Trips Table.
    """
    df = silver_cleaned_yellow_taxi_trips
    return Output(df, metadata={"row_count": df.count(), "column_names": df.columns})
