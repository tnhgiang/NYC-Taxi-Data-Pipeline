# Reference: https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/definitions.py # noqa: E501
# Reference: https://github.com/dagster-io/dagster/blob/master/examples/with_pyspark_emr/with_pyspark_emr/definitions.py  # noqa: E501
import os

from dagster import Definitions, load_assets_from_package_module
from dagster_dbt import DbtCliResource
from dagster_pyspark import PySparkResource

from pipeline import assets

from .dbt import dbt_project
from .resources.clickhouse_io_manager import ClickHouseIOManager
from .resources.file_downloader_resource import (
    CSVDownloaderResource,
    ZipFileDownloaderResource,
)
from .resources.minio_io_manager import (
    MinIOCSVIOManager,
    MinIOPartitionedParquetIOManager,
    MinIOZippedShapefileIOManager,
)
from .resources.mysql_io_manager import MySQLIOManager
from .resources.spark_io_manager import SparkPartitionedParquetIOManager

####################
#     Assets       #
####################
pipeline_assets = load_assets_from_package_module(assets)

# dbt
# DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_nyc")
# DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_nyc")
# dbt_asset = load_assets_from_dbt_project(
#     profiles_dir=DBT_PROFILES_DIR,
#     project_dir=DBT_PROJECT_DIR,
#     key_prefix=["warehouse", "nyc_taxi"],
# )

# Combine all assets
all_assets = [*pipeline_assets]


####################
#  Configuration   #
####################
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ROOT_USER"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
    "bucket": os.getenv("MINIO_DATALAKE_BUCKET"),
    "create_bucket_if_not_exists": (False if not os.getenv("ENV") else True),
}

SPARK_CONFIG = {
    "spark.app.name": os.getenv("SPARK_APP_NAME"),
    "spark.master": os.getenv("SPARK_MASTER_URL"),
    "spark.hadoop.fs.s3a.endpoint": f"http://{os.getenv('MINIO_ENDPOINT')}",
    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ROOT_USER"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_ROOT_PASSWORD"),
    "spark.sql.catalog.clickhouse.host": os.getenv("CLICKHOUSE_HOST"),
    "spark.sql.catalog.clickhouse.http_port": os.getenv("CLICKHOUSE_HTTP_PORT"),
    "spark.sql.catalog.clickhouse.user": os.getenv("CLICKHOUSE_USER"),
    "spark.sql.catalog.clickhouse.password": os.getenv("CLICKHOUSE_PASSWORD"),
    "spark.sql.catalog.clickhouse.database": os.getenv("CLICKHOUSE_DB"),
}

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST"),
    "port": os.getenv("CLICKHOUSE_TCP_PORT"),
    "user": os.getenv("CLICKHOUSE_USER"),
    "password": os.getenv("CLICKHOUSE_PASSWORD"),
    "database": os.getenv("CLICKHOUSE_DB"),
}

####################
#    Resources     #
####################
pyspark_resource = PySparkResource(spark_config=SPARK_CONFIG)
RESOURCES_LOCAL = {
    "pyspark": pyspark_resource,
    "csv_downloader": CSVDownloaderResource(),
    "zipfile_downloader": ZipFileDownloaderResource(),
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "csv_io_manager": MinIOCSVIOManager(MINIO_CONFIG),
    "shapefile_io_manager": MinIOZippedShapefileIOManager(MINIO_CONFIG),
    "parquet_io_manager": MinIOPartitionedParquetIOManager(MINIO_CONFIG),
    "spark_io_manager": SparkPartitionedParquetIOManager(pyspark=pyspark_resource),
    "warehouse_io_manager": ClickHouseIOManager(CLICKHOUSE_CONFIG),
    "dbt": DbtCliResource(project_dir=dbt_project, target="local"),
}


# TODO: Add resources for staging and production environments
RESOURCES_STAGING = {}
RESOURCES_PROD = {}

resources_by_deployment_env = {
    "LOCAL": RESOURCES_LOCAL,
    "STAGING": RESOURCES_STAGING,
    "PROD": RESOURCES_PROD,
}
deployment_env = os.getenv("ENV", "LOCAL")

####################
#   Definitions    #
####################
defs = Definitions(
    assets=all_assets,
    resources=resources_by_deployment_env[deployment_env],
)
