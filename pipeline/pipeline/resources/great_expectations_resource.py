from typing import Union

import pandas as pd
from dagster import ConfigurableResource
from great_expectations.data_context import EphemeralDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from pyspark.sql import DataFrame


class GreatExpectationsResource(ConfigurableResource):
    def get_validator(self, asset_df: Union[DataFrame, pd.DataFrame]):
        """Get a GE validator for a given asset dataframe"""
        # Create GX data context
        project_config = DataContextConfig(
            store_backend_defaults=InMemoryStoreBackendDefaults()
        )
        data_context = EphemeralDataContext(project_config=project_config)

        # Create GX asset, batch and suite
        if isinstance(asset_df, DataFrame):
            data_source = data_context.sources.add_spark(name="datasource")
        else:
            data_source = data_context.sources.add_pandas(name="datasource")

        data_asset = data_source.add_dataframe_asset(name="asset")
        batch_request = data_asset.build_batch_request(dataframe=asset_df)
        data_context.add_or_update_expectation_suite(expectation_suite_name="suite")

        # Create validator
        validator = data_context.get_validator(
            batch_request=batch_request, expectation_suite_name="suite"
        )

        return validator
