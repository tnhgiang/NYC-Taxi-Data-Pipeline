#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi


# Set LOCAL_INFILE in mysql to be true
docker exec ds_mysql \
      mysql \
      -u"root" \
      -p"${MYSQL_ROOT_PASSWORD}" \
      -P"${MYSQL_PORT}" \
      -e"SET GLOBAL local_infile=1;" "${MYSQL_DATABASE}"

# Apply schema into mysql database
docker cp ./scripts/database/mysql_schema.sql ds_mysql:/tmp/
docker exec ds_mysql \
      mysql \
      --local-infile=1 \
      -u"${MYSQL_USER}" \
      -p"${MYSQL_PASSWORD}" \
      -P"${MYSQL_PORT}" \
      -e"source /tmp/mysql_schema.sql;" ${MYSQL_DATABASE}
