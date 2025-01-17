from typing import Iterable

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSpec,
    AssetsDefinition,
    multi_asset_check,
)
from great_expectations.core import ExpectationConfiguration


def multi_spark_asset_check_factory(
    multi_asset_check_name: str,
    specs: dict[str, ExpectationConfiguration],
    asset: AssetsDefinition,
):
    """A factory function to create a multi-asset check function for Spark assets"""

    # TODO: This is the temporary solution to check the partitioned asset
    # As dagster does not support asset check with partition
    # Check here: https://github.com/dagster-io/dagster/issues/17005
    # Manually load spark dataframe from MinIO and use multi_asset_check
    # to reduce the number of spark df loading. But it can not use the blocking feature
    @multi_asset_check(
        name=multi_asset_check_name,
        specs=[AssetCheckSpec(name=name, asset=asset) for name in specs.keys()],
        required_resource_keys={"pyspark", "gx"},
    )
    def multi_asset_check_fn(
        context: AssetCheckExecutionContext,
    ) -> Iterable[AssetCheckResult]:
        layer, schema, table = asset.key.path
        partition_key = context.run.tags["dagster/partition"]
        key = "/".join(
            ["s3a://lake", layer, schema, table.replace(f"{layer}_", ""), partition_key]
        )

        spark = context.resources.pyspark.spark_session
        df = spark.read.parquet(key)

        validator = context.resources.gx.get_validator(df)
        for check_name, expectation_config in specs.items():
            validation_result = expectation_config.validate(validator)

            yield AssetCheckResult(
                passed=validation_result.success,
                metadata=validation_result.result,
                check_name=check_name,
            )

    return multi_asset_check_fn
