import os
from contextlib import contextmanager
from typing import Union

import polars as pl
from minio import Minio

from dagster import InputContext, IOManager, OutputContext

from ..utils import get_current_time


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("access_key"),
        secret_key=config.get("secret_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise


class MinIOPartitionedParquetIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _create_bucket_if_not_exits(self, client, bucket_name):
        """Create a bucket for datalake if it does not exist."""
        try:
            found = client.bucket_exists(bucket_name)
            if not found:
                client.make_bucket(bucket_name)
        except Exception:
            raise

    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Get the path for the parquet file."""
        # Example: layer = bronze, schema=nyc_taxi, table=bronze_yellow_taxi_trips
        layer, schema, table = context.asset_key.path
        # Example: key = datalake/bronze/nyc_taxi/yellow_taxi_trips
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])

        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            get_current_time(), "-".join(context.asset_key.path)
        )

        if context.has_partition_key:
            partition_key = context.asset_partition_key
            # Example: datalake/bronze/nyc_taxi/yellow_taxi_trips/20240101.pq
            return os.path.join(key, f"{partition_key}.pq"), tmp_file_path
        else:
            # Example: datalake/bronze/nyc_taxi/yellow_taxi_trips.pq
            return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        obj.write_parquet(tmp_file_path, use_pyarrow=True)

        # upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                self._create_bucket_if_not_exits(client, bucket_name)
                client.fput_object(bucket_name, key_name, tmp_file_path)

            row_count = len(obj)
            context.add_output_metadata(
                {"path": key_name, "records": row_count, "tmp": tmp_file_path}
            )

            # clean up tmp file
            os.remove(tmp_file_path)
        except Exception:
            raise

    def load_input(self, context: InputContext) -> pl.DataFrame:
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        # download from MinIO
        try:
            with connect_minio(self._config) as client:
                self._create_bucket_if_not_exits(client, bucket_name)
                client.fget_object(bucket_name, key_name, tmp_file_path)

            pl_data = pl.read_parquet(tmp_file_path, use_pyarrow=True)

            return pl_data
        except Exception:
            raise
