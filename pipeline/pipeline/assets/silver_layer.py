import polars as pl
from dagster import AssetExecutionContext, AssetIn, Output, asset
from pyspark.sql import DataFrame
from pyspark.sql.functions import when

from .. import constants as const
from ..partitions import daily_partitions


@asset(
    ins={
        "bronze_yellow_taxi_trips": AssetIn(
            key_prefix=["bronze", "nyc_taxi"],
        )
    },
    name="silver_cleaned_yellow_taxi_trips",
    io_manager_key="spark_io_manager",
    required_resource_keys={"pyspark"},
    key_prefix=["silver", "nyc_taxi"],
    compute_kind="Spark",
    group_name="silver",
    partitions_def=daily_partitions,
)
def silver_cleaned_yellow_taxi_trips(
    context: AssetExecutionContext, bronze_yellow_taxi_trips: pl.DataFrame
) -> Output[DataFrame]:
    """
    The data cleaning and filtering processing based on the Data Dictionary provided at
    https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

    Args:
        context (AssetExecutionContext): The execution context.
        bronze_yellow_taxi_trips (pl.DataFrame): The input DataFrame.

    Returns:
        Output[DataFrame]: The cleaned and filtered DataFrame.
    """
    spark = context.resources.pyspark.spark_session

    # Convert Polars DataFrame to Spark DataFrame
    df = spark.createDataFrame(bronze_yellow_taxi_trips.to_dicts())
    df.cache()

    context.log.info(f"Number of records before data cleaning: {df.count()}")

    # Deduplication
    df = df.dropDuplicates()
    context.log.debug(f"Number of records after deduplication: {df.count()}")

    # Drop redundant columns
    df = df.drop("store_and_fwd_flag")

    # Data type corrections
    df = (
        df.withColumn("VendorID", df["VendorID"].cast("int"))
        .withColumn(
            "tpep_pickup_datetime", df["tpep_pickup_datetime"].cast("timestamp")
        )
        .withColumn(
            "tpep_dropoff_datetime", df["tpep_dropoff_datetime"].cast("timestamp")
        )
        .withColumn("passenger_count", df["passenger_count"].cast("int"))
        .withColumn("trip_distance", df["trip_distance"].cast("float"))
        .withColumn("PULocationID", df["PULocationID"].cast("int"))
        .withColumn("DOLocationID", df["DOLocationID"].cast("int"))
        .withColumn("RatecodeID", df["RatecodeID"].cast("int"))
        .withColumn("payment_type", df["payment_type"].cast("int"))
        .withColumn("fare_amount", df["fare_amount"].cast("float"))
        .withColumn("extra", df["extra"].cast("float"))
        .withColumn("mta_tax", df["mta_tax"].cast("float"))
        .withColumn("improvement_surcharge", df["improvement_surcharge"].cast("float"))
        .withColumn("tip_amount", df["tip_amount"].cast("float"))
        .withColumn("tolls_amount", df["tolls_amount"].cast("float"))
        .withColumn("total_amount", df["total_amount"].cast("float"))
        .withColumn("congestion_surcharge", df["congestion_surcharge"].cast("float"))
        .withColumn("Airport_fee", df["Airport_fee"].cast("float"))
    )

    # Data range constraints
    # Drop rows where data points break range constraints
    df = (
        df.filter(
            (df["RatecodeID"] >= const.RATECODEID_MIN_VALUE)
            & (df["RatecodeID"] <= const.RATECODEID_MAX_VALUE)
        )
        .filter(df["trip_distance"] >= 0)
        .filter(df["fare_amount"] >= 0)
        .filter(df["extra"] >= 0)
        .filter(df["mta_tax"] >= 0)
        .filter(df["improvement_surcharge"] >= 0)
        .filter(df["tip_amount"] >= 0)
        .filter(df["tolls_amount"] >= 0)
        .filter(df["total_amount"] >= 0)
        .filter(df["congestion_surcharge"] >= 0)
        .filter(df["Airport_fee"] >= 0)
    )
    context.log.debug(f"Number of records after data range constraints: {df.count()}")

    # Set data point that breaks range constraints to default value
    df = df.withColumn(
        "payment_type",
        when(
            (df["payment_type"] >= const.PAYMENT_TYPE_MIN_VALUE)
            & (df["payment_type"] <= const.PAYMENT_TYPE_MAX_VALUE),
            const.UNKNOWN_PAYMENT_TYPE_VALUE,
        ).otherwise(df["payment_type"]),
    )

    # Rename columns
    column_names_need_to_be_renamed_list = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "Airport_fee",
    ]
    new_column_names_list = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "airport_fee",
    ]
    for idx in range(len(column_names_need_to_be_renamed_list)):
        df = df.withColumnRenamed(
            column_names_need_to_be_renamed_list[idx], new_column_names_list[idx]
        )

    # Add a new column for trip_duration in minutes
    trip_duration_in_minutes = (
        df["dropoff_datetime"].cast("long") - df["pickup_datetime"].cast("long")
    ) / 60
    df = df.withColumn("trip_duration", trip_duration_in_minutes)

    context.log.info(f"Number of records after data cleaning: {df.count()}")
    df.unpersist()

    return Output(df, metadata={"row_count": df.count(), "column_names": df.columns})
