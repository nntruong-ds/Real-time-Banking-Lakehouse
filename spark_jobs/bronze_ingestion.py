"""
Spark job Bronze — nhận giao dịch từ Kafka, kiểm tra luật nghiệp vụ, tách 2 ngả:

                                    ┌─► hợp lệ ─► lakehouse.bronze.transactions_raw
    Kafka "transactions" ─► kiểm tra┤
                                    └─► lỗi ────► lakehouse.bronze.dlq
                                                └► Kafka topic "dlq_transactions"

Ba ngả ghi là 3 streaming query độc lập, mỗi query một checkpoint riêng:
  - 2 query ghi Iceberg: exactly-once (checkpoint + commit nguyên tử của Iceberg)
  - 1 query ghi Kafka DLQ: at-least-once -> bên đọc DLQ cần bỏ trùng.

Bronze giữ NGUYÊN TRẠNG (kể cả PII và bản trùng) để đối chiếu và nạp lại khi cần;
việc bỏ trùng và che PII làm ở Silver.

Chạy: service "bronze-stream" trong docker-compose (profile realtime).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.streaming import (DLQ_TOPIC, KAFKA_BOOTSTRAP, SOURCE_TOPIC, TRIGGER, checkpoint,
                              read_validated_kafka, run_until_stopped)
from common.tables import (BRONZE_TABLE, DLQ_TABLE, create_tables, invalid_rows,
                           valid_rows)


def main():
    spark = SparkSession.builder.appName("bronze_ingestion").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)
    print(f"[bronze] Bang {BRONZE_TABLE} va {DLQ_TABLE} da san sang", flush=True)

    validated = read_validated_kafka(spark)
    invalid = invalid_rows(validated)

    # Ngả 1 — bản ghi hợp lệ vào Bronze (exactly-once)
    q_valid = (valid_rows(validated).writeStream
               .format("iceberg").outputMode("append")
               .trigger(processingTime=TRIGGER)
               .option("fanout-enabled", "true")
               .option("checkpointLocation", checkpoint("bronze_transactions"))
               .queryName("bronze_valid")
               .toTable(BRONZE_TABLE))

    # Ngả 2 — bản ghi lỗi vào bảng DLQ (exactly-once)
    q_dlq = (invalid.writeStream
             .format("iceberg").outputMode("append")
             .trigger(processingTime=TRIGGER)
             .option("fanout-enabled", "true")
             .option("checkpointLocation", checkpoint("bronze_dlq_iceberg"))
             .queryName("dlq_iceberg")
             .toTable(DLQ_TABLE))

    # Ngả 3 — bản ghi lỗi vào Kafka DLQ để hệ thống khác xử lý lại (at-least-once)
    dlq_message = F.to_json(F.struct(
        "error_reason",
        F.col("raw_value").alias("original_record"),
        "kafka_partition",
        "kafka_offset",
        F.date_format("ingest_time", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("ingest_time"),
    ))
    q_kafka = (invalid
               .select(F.col("transaction_id").alias("key"), dlq_message.alias("value"))
               .writeStream
               .format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
               .option("topic", DLQ_TOPIC)
               .trigger(processingTime=TRIGGER)
               .option("checkpointLocation", checkpoint("bronze_dlq_kafka"))
               .queryName("dlq_kafka")
               .start())

    print(f"[bronze] Dang chay 3 query: '{SOURCE_TOPIC}' -> {BRONZE_TABLE} | {DLQ_TABLE} | "
          f"Kafka '{DLQ_TOPIC}', moi {TRIGGER}. Bam Ctrl+C de dung.", flush=True)
    run_until_stopped([q_valid, q_dlq, q_kafka], "bronze")


if __name__ == "__main__":
    main()
