import polars as pl
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
    asset_check,
)
from great_expectations.core import ExpectationConfiguration

from ..assets.silver_layer import (
    silver_cleaned_taxi_zone,
    silver_cleaned_taxi_zone_geometry,
    silver_cleaned_yellow_taxi_trips,
)
from .factory import multi_spark_asset_check_factory

# Checks for cleaned yellow taxi trips
# TODO: Just there are some critical checks for minimal report, add more checks
multi_asset_check_specs = {
    "validity": {
        "validity_pickup_datetime_to_be_datetime": ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "pickup_datetime", "type_": "TimestampType"},
        ),
        "validity_dropoff_datetime_to_be_datetime": ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "pickup_datetime", "type_": "TimestampType"},
        ),
        "validity_passenger_count_to_be_positive": ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "passenger_count", "min_value": 0, "strict_min": True},
        ),
        "validity_total_amount_to_be_positive": ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "total_amount", "min_value": 0, "strict_min": True},
        ),
    },
    "completeness": {
        "completeness_trip_key_not_be_null": ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "trip_key"},
        ),
        "completeness_vendor_not_be_null": ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "vendor"},
        ),
    },
    "uniqueness": {
        "uniqueness_trip_key_to_be_unique": ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "trip_key"},
        )
    },
}


silver_cleaned_yellow_taxi_trips_checks = [
    multi_spark_asset_check_factory(*checks, silver_cleaned_yellow_taxi_trips)
    for checks in multi_asset_check_specs.items()
]

# Check for cleaned taxi zone
asset_check_specs = {
    "completeness_location_id_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "location_id"},
    ),
    "completeness_borouth_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "borough"},
    ),
    "completeness_zone_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "zone"},
    ),
    "uniqueness_location_id_to_be_unique": ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "location_id"},
    ),
}


def silver_cleaned_taxi_zone_asset_check_factory(
    expectation_name: str,
    expectation_config: ExpectationConfiguration,
) -> AssetChecksDefinition:
    @asset_check(
        asset=silver_cleaned_taxi_zone,
        name=expectation_name,
        blocking=True,
        required_resource_keys={"gx"},
        compute_kind="great_expectations",
    )
    def _asset_check(
        context: AssetCheckExecutionContext, df: pl.DataFrame
    ) -> AssetCheckResult:
        validator = context.resources.gx.get_validator(df.to_pandas())
        validation_result = expectation_config.validate(validator)

        return AssetCheckResult(
            passed=validation_result.success, metadata=validation_result.result
        )

    return _asset_check


silver_cleaned_taxi_zone_checks = [
    silver_cleaned_taxi_zone_asset_check_factory(*checks)
    for checks in asset_check_specs.items()
]

# Checks for cleaned taxi zone geometry
asset_check_specs = {
    "completeness_location_id_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "location_id"},
    ),
    "completeness_borough_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "borough"},
    ),
    "completeness_zone_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "zone"},
    ),
    "completeness_latitude_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "latitude"},
    ),
    "completeness_longitude_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "longitude"},
    ),
    "completeness_shape_length_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "shape_length"},
    ),
    "completeness_shape_area_not_be_null": ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "shape_area"},
    ),
}


def silver_cleaned_taxi_zone_geometry_asset_check_factory(
    expectation_name: str,
    expectation_config: ExpectationConfiguration,
) -> AssetChecksDefinition:
    @asset_check(
        asset=silver_cleaned_taxi_zone_geometry,
        name=expectation_name,
        blocking=True,
        required_resource_keys={"gx"},
        compute_kind="great_expectations",
    )
    def _asset_check(
        context: AssetCheckExecutionContext, df: pl.DataFrame
    ) -> AssetCheckResult:
        validator = context.resources.gx.get_validator(df.to_pandas())
        validation_result = expectation_config.validate(validator)

        return AssetCheckResult(
            passed=validation_result.success, metadata=validation_result.result
        )

    return _asset_check


silver_cleaned_taxi_zone_geometry_checks = [
    silver_cleaned_taxi_zone_geometry_asset_check_factory(*checks)
    for checks in asset_check_specs.items()
]

silver_checks = (
    *silver_cleaned_yellow_taxi_trips_checks,
    *silver_cleaned_taxi_zone_checks,
    *silver_cleaned_taxi_zone_geometry_checks,
)
