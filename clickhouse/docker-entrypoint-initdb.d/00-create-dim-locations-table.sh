#!/bin/bash
# set -e

echo "Creating table: dim_locations"

clickhouse client -n <<EOSQL
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.dim_locations
(
    location_id UInt32,
    borough String,
    zone String,
    shape_length Float32,
    shape_area Float32,
    longitude Float32,
    latitude Float32
)
ENGINE = MergeTree()
PRIMARY KEY location_id;
EOSQL
