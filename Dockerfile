FROM python:3.12-slim-bookworm

ARG SPARK_VERSION=4.0.1
ARG HADOOP_VERSION=3
ARG DELTA_VERSION=4.0.0
ARG SCALA_VERSION=2.13
ARG ANTLR_VERSION=4.13.1
ARG SPARK_DOWNLOAD_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ARG DELTA_JAR_URL=https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar

RUN apt-get update && apt-get install -y curl default-jre procps && \
    curl -s ${SPARK_DOWNLOAD_URL} -o spark.tgz && \
    tar -xvf spark.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz && \
    mkdir -p /opt/spark/jars && \
    curl -s ${DELTA_JAR_URL} -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar && \
    curl -s https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar && \
    curl -s https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/${ANTLR_VERSION}/antlr4-runtime-${ANTLR_VERSION}.jar -o /opt/spark/jars/antlr4-runtime-${ANTLR_VERSION}.jar && \
    rm -f /opt/spark/jars/antlr-runtime-${SPARK_VERSION}.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

RUN pip install --no-cache-dir pyspark==${SPARK_VERSION} delta-spark==${DELTA_VERSION} jupyterlab

COPY conf ${SPARK_HOME}/conf
COPY conf/log4j2.properties ${SPARK_HOME}/conf/log4j2.properties
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh