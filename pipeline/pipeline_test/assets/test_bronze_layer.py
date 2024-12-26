import polars as pl
from dagster import (
    InMemoryIOManager,
    InputContext,
    IOManager,
    OutputContext,
    build_op_context,
    materialize,
)

from pipeline.assets.bronze_layer import bronze_yellow_taxi_trips


class MockMySQLIOManager(IOManager):

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        # Return a dummy DataFrame
        data = {
            "trip_id": [1, 2, 3],
            "VendorID": [1, 2, 1],
            "tpep_pickup_datetime": [
                "2021-01-01 00:15:56",
                "2021-01-01 00:21:22",
                "2021-01-01 00:35:00",
            ],
            "tpep_dropoff_datetime": [
                "2021-01-01 00:25:56",
                "2021-01-01 00:31:22",
                "2021-01-01 00:45:00",
            ],
            "passenger_count": [1, 2, 3],
            "trip_distance": [1.5, 2.3, 3.1],
            "RatecodeID": [1, 1, 2],
            "store_and_fwd_flag": ["N", "Y", "N"],
            "PULocationID": [130, 132, 135],
            "DOLocationID": [205, 208, 210],
            "payment_type": [1, 2, 1],
            "fare_amount": [12.0, 15.5, 18.0],
            "extra": [0.5, 1.0, 0.5],
            "mta_tax": [0.5, 0.5, 0.5],
            "tip_amount": [2.0, 3.0, 2.5],
            "tolls_amount": [0.0, 0.0, 0.0],
            "improvement_surcharge": [0.3, 0.3, 0.3],
            "total_amount": [15.3, 20.3, 21.8],
            "congestion_surcharge": [2.5, 2.5, 2.5],
            "Airport_fee": [0.0, 0.0, 1.75],
        }
        return pl.DataFrame(data)


def test_bronze_yellow_taxi_trips():
    result = materialize(
        [bronze_yellow_taxi_trips],
        resources={
            "parquet_io_manager": InMemoryIOManager(),
            "mysql_io_manager": MockMySQLIOManager(),
        },
        partition_key="2024-01-01"
    )
    assert result.success

    # Create context for testing
    context = build_op_context(
        resources={
            "parquet_io_manager": InMemoryIOManager(),
            "mysql_io_manager": MockMySQLIOManager(),
        },
        partition_key="2024-01-01"
    )
    result = bronze_yellow_taxi_trips(context)
    assert isinstance(result.value, pl.DataFrame)
