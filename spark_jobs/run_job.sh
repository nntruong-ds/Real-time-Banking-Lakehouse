#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Chạy một Spark job trong container spark-iceberg.
#
#   docker exec -it spark-iceberg bash /home/iceberg/spark_jobs/run_job.sh bronze_ingestion.py
#
# Script tự dò phiên bản Spark/Scala trong image rồi tải đúng bản thư viện
# đọc Kafka (spark-sql-kafka). Nhờ vậy image có cập nhật cũng không bị lệch.
# -----------------------------------------------------------------------------
set -euo pipefail

JOB="${1:?Hay truyen ten file job, vi du: bronze_ingestion.py}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"

CORE_JAR="$(basename "$(ls "${SPARK_HOME}"/jars/spark-core_*.jar | head -n 1)")"
# Tên file dạng: spark-core_2.13-4.0.1.jar
SCALA_VERSION="$(echo "${CORE_JAR}" | sed -E 's/^spark-core_([0-9.]+)-.*$/\1/')"
SPARK_VERSION="$(echo "${CORE_JAR}" | sed -E 's/^spark-core_[0-9.]+-(.+)\.jar$/\1/')"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION}"

echo "[run_job] Spark ${SPARK_VERSION}, Scala ${SCALA_VERSION}"
echo "[run_job] Thu vien Kafka: ${KAFKA_PKG}"
echo "[run_job] Lan dau chay se mat vai phut de tai thu vien..."

exec spark-submit \
  --master "local[*]" \
  --driver-memory 2g \
  --packages "${KAFKA_PKG}" \
  --conf spark.jars.ivy=/home/iceberg/.ivy2 \
  --conf spark.sql.shuffle.partitions=4 \
  "/home/iceberg/spark_jobs/${JOB}"
