import os
from contextlib import contextmanager
from typing import Union

import geopandas as gpd
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


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        self._create_bucket_if_not_exists(self._config.get("bucket"))

    def _create_bucket_if_not_exists(self, bucket_name):
        """Create a bucket for lake if it does not exist."""
        with connect_minio(self._config) as client:
            try:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)
            except Exception:
                raise ValueError(
                    f"{self.__class__.__name__}: Failed to create {bucket_name} bucket"
                )

    def _load_file(self, path):
        """Load the file to a DataFrame."""
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj: Union[pl.DataFrame, str]):
        key_name, tmp_file_path = self._get_path(context)
        if isinstance(obj, pl.DataFrame):
            obj.write_parquet(tmp_file_path, use_pyarrow=True)
        else:
            tmp_file_path = obj

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
        except Exception as e:
            context.log.error(
                f"{self.__class__.__name__}: Failed to handle output due to {str(e)}"
            )
            raise
        finally:
            # clean up tmp file
            os.remove(tmp_file_path)

    def load_input(
        self, context: InputContext
    ) -> Union[pl.DataFrame, gpd.GeoDataFrame]:
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        # download from MinIO
        try:
            with connect_minio(self._config) as client:
                client.fget_object(bucket_name, key_name, tmp_file_path)
                context.log.debug(
                    f"{self.__class__.__name__}: Download successfully {key_name} "
                    f"into {tmp_file_path}"
                )

                df = self._load_file(tmp_file_path)
                context.log.debug(
                    f"{self.__class__.__name__}: Loaded {key_name} successfully"
                )

            return df
        except Exception as e:
            context.log.error(
                f"{self.__class__.__name__}: Failed to load input due to {str(e)}"
            )
            raise
        finally:
            # clean up tmp file
            os.remove(tmp_file_path)


class MinIOPartitionedParquetIOManager(MinIOIOManager):
    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Get the path for the CSV file."""
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

    def _load_file(self, path):
        """Load parquet file to a DataFrame."""
        return pl.read_parquet(path, use_pyarrow=True)


class MinIOCSVIOManager(MinIOIOManager):
    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Get the path for the csv file."""
        # Example: layer = bronze, schema=nyc_taxi, table=bronze_yellow_taxi_trips
        layer, schema, table = context.asset_key.path
        # Example: key = bronze/nyc_taxi/yellow_taxi_trips
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.csv".format(
            get_current_time(), "-".join(context.asset_key.path)
        )

        # Example: bronze/nyc_taxi/yellow_taxi_trips.pq
        return f"{key}.csv", tmp_file_path

    def _load_file(self, path):
        """Load the CSV file to a DataFrame."""
        return pl.read_csv(path)


class MinIOZippedShapefileIOManager(MinIOIOManager):
    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Get the path for the shapefile."""
        # Example: layer = bronze, schema=nyc_taxi, table=bronze_yellow_taxi_trips
        layer, schema, table = context.asset_key.path
        # Example: key = bronze/nyc_taxi/yellow_taxi_trips
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.zip".format(
            get_current_time(), "-".join(context.asset_key.path)
        )

        # Example: bronze/nyc_taxi/yellow_taxi_trips
        return f"{key}.zip", tmp_file_path

    def _load_file(self, path):
        """Load the shapefile to a GeoDataFrame."""
        return gpd.read_file(path)
