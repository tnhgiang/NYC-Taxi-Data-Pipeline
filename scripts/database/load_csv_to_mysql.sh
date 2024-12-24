#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Check if correct number of arguments is provided
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <csv_file_path>"
  exit 1
fi

CSV_FILE=$1
# Check if the CSV file exists
if [ ! -f "$CSV_FILE" ]; then
  echo "ERROR: CSV file $CSV_FILE does not exist."
  exit 1
fi

# Check if "yellow" is in the CSV file path and set the table name accordingly
if [[ "$CSV_FILE" == *"yellow"* ]]; then
  TABLE_NAME="yellow_taxi_trips"
else
  echo "ERROR: Not supported green taxi trip CSV file."
  exit 1
fi

# Copy CSV file into mysql container
docker cp $CSV_FILE ds_mysql:/tmp/

# Extract the csv file name
FILE_NAME=$(basename "$CSV_FILE")

# Load CSV file into mysql database
docker exec ds_mysql \
      mysql \
      --local-infile=1 \
      -u"${MYSQL_USER}" \
      -p"${MYSQL_PASSWORD}" \
      -P"${MYSQL_PORT}" \
      -e"LOAD DATA LOCAL INFILE '/tmp/$FILE_NAME'
        INTO TABLE $TABLE_NAME
        FIELDS TERMINATED BY ','
        ENCLOSED BY '\"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS
        (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
          passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
          PULocationID, DOLocationID, payment_type, fare_amount, extra,
          mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount,
          congestion_surcharge, Airport_fee)
        SET trip_id = NULL;" \
      ${MYSQL_DATABASE}

echo "INFO: Loaded successfully $CSV_FILE into mysql database"
