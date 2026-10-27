"""
Spark job Silver — làm sạch, bỏ trùng và che dữ liệu nhạy cảm.

    Kafka "transactions" ─► kiểm tra (cùng luật với Bronze) ─► chỉ giữ bản ghi hợp lệ
                         ─► bỏ trùng ─► che PII ─► lakehouse.silver.transactions

Vì sao Silver đọc Kafka chứ không đọc bảng Bronze (quyết định thiết kế):
  Nguồn streaming Iceberg ghi file offset ban đầu vào thư mục checkpoint bằng FileIO
  của BẢNG. Nessie (REST catalog) luôn gửi kèm mỗi bảng io-impl = S3FileIO, ghi đè
  mọi cấu hình phía Spark, mà S3FileIO không ghi được vào checkpoint cục bộ (file:/)
  -> job sập với "Invalid S3 URI". Vì vậy Bronze và Silver cùng đọc một topic Kafka,
  mỗi job một checkpoint riêng. Bronze vẫn là bản lưu thô đầy đủ để đối chiếu.

Bỏ trùng: nhớ các transaction_id đã thấy trong DEDUP_WATERMARK tính theo THỜI GIAN
GIAO DỊCH. Producer tua nhanh 3600 lần (1 giờ dữ liệu / 1 giây), nên watermark để
2 giờ để các partition Kafka lệch nhau vài phút cũng không làm rơi bản ghi đến trễ.

Chạy: service "silver-stream" trong docker-compose (profile realtime).
"""

import os

from pyspark.sql import SparkSession

from common.pii import load_key
from common.streaming import (SOURCE_TOPIC, TRIGGER, checkpoint, read_validated_kafka,
                              run_until_stopped)
from common.tables import SILVER_TABLE, create_tables, dedup_stream, to_silver, valid_rows

WATERMARK = os.getenv("DEDUP_WATERMARK", "2 hours")


def main():
    key = load_key()   # dừng ngay với thông báo rõ ràng nếu chưa tạo .env

    spark = SparkSession.builder.appName("silver_transformation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)
    print(f"[silver] Bang {SILVER_TABLE} da san sang", flush=True)

    silver = to_silver(dedup_stream(valid_rows(read_validated_kafka(spark)), WATERMARK), key)

    query = (silver.writeStream
             .format("iceberg").outputMode("append")
             .trigger(processingTime=TRIGGER)
             .option("fanout-enabled", "true")
             .option("checkpointLocation", checkpoint("silver_from_kafka"))
             .queryName("silver")
             .toTable(SILVER_TABLE))

    print(f"[silver] Dang doc Kafka '{SOURCE_TOPIC}' -> ghi {SILVER_TABLE} moi {TRIGGER}. "
          "Bam Ctrl+C de dung.", flush=True)
    run_until_stopped([query], "silver")


if __name__ == "__main__":
    main()
