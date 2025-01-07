FROM python:3.12-slim

# Add repository code
WORKDIR /opt/dagster/app

# Set up Spark
# Reference: https://github.com/jupyter/docker-stacks/blob/main/images/pyspark-notebook/Dockerfile
# Spark dependencies
ARG openjdk_version="17"
RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/usr/local/spark
ENV PATH="${PATH}:${SPARK_HOME}/bin"
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"
RUN curl -O https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    tar zxvf spark-3.5.4-bin-hadoop3.tgz -C /usr/local/ && \
    rm -rf spark-3.5.4-bin-hadoop3.tgz && \
    ln -s /usr/local/spark-3.5.4-bin-hadoop3 ${SPARK_HOME}

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
        clickhouse-data-0.7.2.jar /usr/local/spark/jars

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline.definitions"]
