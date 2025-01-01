import polars as pl
from dagster import AssetExecutionContext, AssetIn, Output, asset

from ..partitions import daily_partitions


@asset(
    ins={
        "bronze_yellow_taxi_trips": AssetIn(
            key_prefix=["bronze", "nyc_taxi"],
        )
    },
    name="silver_test",
    io_manager_key="spark_io_manager",
    required_resource_keys={"pyspark"},
    key_prefix=["silver", "nyc_taxi"],
    compute_kind="Spark",
    group_name="silver",
    partitions_def=daily_partitions,
)
def silver_test(
    context: AssetExecutionContext, bronze_yellow_taxi_trips: pl.DataFrame
) -> Output:
    spark = context.resources.pyspark.spark_session

    # TODO: Implement the transformation logic

    # Testing dataframe
    # list  of college data with two lists
    data = [["java", "dbms", "python"],
            ["OOPS", "SQL", "Machine Learning"]]

    # giving column names of dataframe
    columns = ["Subject 1", "Subject 2", "Subject 3"]

    # creating a dataframe
    dataframe = spark.createDataFrame(data, columns)

    return Output(dataframe)
