import os

from dagster import Definitions, load_assets_from_package_module

from pipeline import assets  # noqa: TID252

from .resources.mysql_io_manager import MySQLIOManager
from .resources.parquet_io_manager import MinIOPartitionedParquetIOManager

####################
#     Assets       #
####################
pipeline_assets = load_assets_from_package_module(assets)
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
}


####################
#    Resources     #
####################
RESOURCES_LOCAL = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "parquet_io_manager": MinIOPartitionedParquetIOManager(MINIO_CONFIG),
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
