#!/bin/bash
# set -e

echo "Creating table: dim_locations"

clickhouse client -n <<EOSQL
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.dim_locations
(
    location_id UInt32,
    location_name String,
    location_type String
)
ENGINE = MergeTree()
PRIMARY KEY location_id;
EOSQL
