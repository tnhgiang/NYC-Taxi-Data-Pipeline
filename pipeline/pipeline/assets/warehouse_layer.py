from typing import Any, Mapping

import polars as pl
from dagster import AssetExecutionContext, AssetIn, AssetKey, Output, asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets
from pyspark.sql import DataFrame

from ..dbt import dbt_project
from ..partitions import daily_partitions


@asset(
    ins={
        "fact_trips": AssetIn(
            key_prefix=["gold", "nyc_taxi"],
        )
    },
    name="fact_trips",
    key_prefix=["warehouse", "nyc_taxi"],
    group_name="warehouse",
    io_manager_key="warehouse_io_manager",
    metadata={
        "columns": [
            "trip_id",
            "vendor",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "trip_duration",
            "pickup_location_id",
            "dropoff_location_id",
            "ratecode",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "improvement_surcharge",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ],
        "primary_keys": ["trip_id"],
    },
    compute_kind="ClickHouse",
    partitions_def=daily_partitions,
)
def fact_trips(context: AssetExecutionContext, fact_trips: DataFrame):
    """
    Create the Fact Trips Table.
    """
    return Output(fact_trips, metadata={"row_count": fact_trips.count()})


@asset(
    ins={
        "dim_locations": AssetIn(
            key_prefix=["gold", "nyc_taxi"],
        )
    },
    name="dim_locations",
    key_prefix=["warehouse", "nyc_taxi"],
    group_name="warehouse",
    io_manager_key="warehouse_io_manager",
    metadata={
        "columns": [
            "location_id",
            "borough",
            "zone",
            "shape_length",
            "shape_area",
            "longitude",
            "latitude",
        ],
        "primary_keys": ["location_id"],
    },
    compute_kind="ClickHouse",
)
def dim_locations(context: AssetExecutionContext, dim_locations: pl.DataFrame):
    """
    Create the Fact Trips Table.
    """
    return Output(dim_locations, metadata={"row_count": dim_locations.height})


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return (
            super()
            .get_asset_key(dbt_resource_props)
            .with_prefix(["warehouse", "nyc_taxi"])
        )


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def warehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
