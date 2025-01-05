#!/bin/bash
# set -e

echo "Creating table: fact_trips"

clickhouse client -n <<EOSQL
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.fact_trips
(
    trip_id UInt32,
    vendor String,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Float32,
    pickup_location_id Float32,
    dropoff_location_id Float32,
    rate_code String,
    payment_type String,
    fare_amount Float32,
    extra Float32,
    mta_tax Float32,
    tip_amount Float32,
    tolls_amount Float32,
    total_amount Float32,
    congestion_surcharge Float32,
    airport_fee Float32
)
ENGINE = MergeTree()
PRIMARY KEY trip_id;
EOSQL
