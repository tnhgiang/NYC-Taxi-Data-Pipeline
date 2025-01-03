import os
from contextlib import contextmanager
from typing import Union

import polars as pl
from dagster import InputContext, IOManager, OutputContext
from minio import Minio

from ..utils import get_current_time, get_size_in_MB


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
        self._create_bucket_if_not_exits(self._config.get("bucket"))

    def _create_bucket_if_not_exits(self, bucket_name):
        """Create a bucket for lake if it does not exist."""
        with connect_minio(self._config) as client:
            try:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
            except Exception:
                raise ValueError(
                    f"{self.__class__.__name__}: Failed to create{bucket_name} bucket"
                )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Get the path for the parquet file."""
        # Example: layer = bronze, schema=nyc_taxi, table=bronze_yellow_taxi_trips
        layer, schema, table = context.asset_key.path
        # Example: key = bronze/nyc_taxi/yellow_taxi_trips
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])

        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            get_current_time(), "-".join(context.asset_key.path)
        )

        if context.has_partition_key:
            partition_key = context.asset_partition_key
            # Example: bronze/nyc_taxi/yellow_taxi_trips/20240101.pq
            return os.path.join(key, f"{partition_key}.pq"), tmp_file_path
        else:
            # Example: bronze/nyc_taxi/yellow_taxi_trips.pq
            return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        if not isinstance(obj, pl.DataFrame):
            raise ValueError(
                f"{self.__class__.__name__}: Output obj should be a Polars DataFrame, "
                f"got {type(obj)} instead."
            )
        # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        obj.write_parquet(tmp_file_path, use_pyarrow=True)

        # upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                client.fput_object(bucket_name, key_name, tmp_file_path)
                context.log.debug(
                    f"{self.__class__.__name__}: {key_name} saved successfully"
                )

            context.add_output_metadata(
                {
                    "path": key_name,
                    "tmp": tmp_file_path,
                    "size_on_disk_in_MB": get_size_in_MB(tmp_file_path),
                }
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
                client.fget_object(bucket_name, key_name, tmp_file_path)
                context.log.debug(
                    f"{self.__class__.__name__}: {key_name} loaded successfully"
                )

            pl_data = pl.read_parquet(tmp_file_path, use_pyarrow=True)
            if not isinstance(pl_data, pl.DataFrame):
                raise ValueError(
                    f"{self.__class__.__name__}: Output obj should be a "
                    f"Polars DataFrame, got {type(pl_data)} instead."
                )

            return pl_data
        except Exception:
            raise
