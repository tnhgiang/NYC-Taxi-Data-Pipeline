# Reference: https://github.com/ClickHouse/metabase-clickhouse-driver/blob/master/.build/Dockerfile
FROM metabase/metabase:v0.51.10

# Install clickhouse driver
RUN curl -L -o /plugins/clickhouse.metabase-driver.jar https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.51.0/clickhouse.metabase-driver.jar && \
    chmod 744 /plugins/clickhouse.metabase-driver.jar
