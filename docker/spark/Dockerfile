# =============================================================================
# Image Spark của dự án: Spark 3.5 + Python 3 + Iceberg + Kafka connector.
#
# Mọi phiên bản khai báo ở MỘT chỗ (các ARG dưới đây). Thư viện được tải sẵn
# vào image lúc build, nên job chạy không cần Internet và luôn đúng một phiên bản.
#
# Build context là thư mục gốc dự án:   docker compose build
# =============================================================================
ARG SPARK_IMAGE=spark:3.5.9-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG SPARK_VERSION=3.5.9
ARG SCALA_VERSION=2.12
ARG SPARK_MINOR=3.5
ARG ICEBERG_VERSION=1.10.0
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# ---- Thư viện Java: Iceberg (đọc/ghi bảng) + AWS bundle (S3) + Kafka connector ----
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar /opt/spark/jars/

# ---- Thư viện Python: để chạy unit test trong container ----
COPY docker/spark/requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt \
 && rm /tmp/requirements-spark.txt

# ---- Cho Python thấy pyspark có sẵn trong /opt/spark/python ----
RUN ln -s "$(ls /opt/spark/python/lib/py4j-*-src.zip | head -n 1)" /opt/spark/python/lib/py4j-src.zip
ENV PYTHONPATH=/opt/app/spark_jobs:/opt/spark/python:/opt/spark/python/lib/py4j-src.zip \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/spark/bin:${PATH}

# ---- Cấu hình Spark: catalog Iceberg tên "lakehouse" trỏ tới Nessie ----
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# ---- Code của dự án (khi chạy, docker-compose gắn đè từ máy để sửa code không cần build lại) ----
COPY --chown=spark:spark spark_jobs/ /opt/app/spark_jobs/
COPY --chown=spark:spark tests/ /opt/app/tests/
COPY --chown=spark:spark data_generator/ /opt/app/data_generator/

# Thư mục checkpoint; volume gắn vào đây sẽ nhận quyền của user spark
RUN mkdir -p /opt/spark/checkpoints \
 && chown -R spark:spark /opt/spark/checkpoints /opt/app

USER spark
WORKDIR /opt/app
