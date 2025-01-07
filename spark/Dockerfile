FROM docker.io/bitnami/spark:3.5

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl

# Install additional JAR files for connecting to MinIO and Delta Lake
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -O https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/0.8.1/clickhouse-spark-runtime-3.5_2.12-0.8.1.jar && \
    curl -O https://repo1.maven.org/maven2/com/clickhouse/clickhouse-client/0.7.2/clickhouse-client-0.7.2.jar && \
    curl -O https://repo1.maven.org/maven2/com/clickhouse/clickhouse-http-client/0.7.2/clickhouse-http-client-0.7.2.jar && \
    curl -O https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.4.1/httpclient5-5.4.1.jar && \
    curl -O https://repo1.maven.org/maven2/com/clickhouse/clickhouse-data/0.7.2/clickhouse-data-0.7.2.jar && \
    mv s3-2.18.41.jar aws-java-sdk-1.12.367.jar aws-java-sdk-bundle-1.11.1026.jar delta-core_2.12-2.2.0.jar \
        delta-storage-2.2.0.jar hadoop-aws-3.3.2.jar clickhouse-spark-runtime-3.5_2.12-0.8.1.jar \
        clickhouse-client-0.7.2.jar clickhouse-http-client-0.7.2.jar httpclient5-5.4.1.jar \
        clickhouse-data-0.7.2.jar /opt/bitnami/spark/jars
