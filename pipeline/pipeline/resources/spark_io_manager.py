# Reference: https://github.com/dagster-io/dagster/blob/master/examples/with_pyspark_emr/with_pyspark_emr/definitions.py # noqa: E501
import os

from dagster import InputContext, IOManager, OutputContext
from dagster_pyspark import PySparkResource
from pyspark.sql import DataFrame


class SparkPartitionedParquetIOManager(IOManager):
    pyspark: PySparkResource

    def _get_path(self, context: OutputContext):
        """Get the path for the parquet file."""
        # Example: layer = silver, schema=nyc_taxi, table=bronze_yellow_taxi_trips
        layer, schema, table = context.asset_key.path
        # Example: key = silver/nyc_taxi/yellow_taxi_trips
        key = "/".join(["s3a://lake", layer, schema, table.replace(f"{layer}_", "")])

        if context.has_partition_key:
            partition_key = context.asset_partition_key
            # Example: silver/nyc_taxi/yellow_taxi_trips/20240101.pq
            return os.path.join(key, f"{partition_key}")
        else:
            return f"{key}"

    def handle_output(self, context: OutputContext, obj: DataFrame):
        if not isinstance(obj, DataFrame):
            raise ValueError(
                f"{self.__class__.__name__}: Output obj should be a Spark DataFrame, "
                f"got {type(obj)} instead."
            )

        try:
            key_name = self._get_path(context)
            obj.write.mode("overwrite").parquet(key_name)
            context.log.debug(
                f"{self.__class__.__name__}: {key_name} saved successfully"
            )
        except Exception:
            raise

    def load_input(self, context: InputContext) -> DataFrame:
        try:
            key_name = self._get_path(context)

            # Load the parquet file into a Spark DataFrame
            spark = self.pyspark.spark_session
            spark_data = spark.read.parquet(key_name)

            if not isinstance(spark_data, DataFrame):
                raise ValueError(
                    f"{self.__class__.__name__}: Output obj should be "
                    f"a Spark DataFrame, got {type(spark_data)} instead."
                )

            return spark_data
        except Exception:
            raise
