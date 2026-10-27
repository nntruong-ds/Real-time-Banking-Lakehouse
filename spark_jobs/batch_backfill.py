"""
Spark job batch — nạp LỊCH SỬ giao dịch từ file CSV (backfill) vào Bronze, DLQ và Silver.

    fraudTrain.csv (2019 -> 06/2020) ─► kiểm tra (cùng luật với stream)
                                     ├─► hợp lệ ─► Bronze ─► bỏ trùng, che PII ─► Silver
                                     └─► lỗi ────► DLQ

Dùng CHUNG code với luồng streaming (common/schema.py, validation.py, tables.py): mỗi dòng
CSV được đổi thành đúng chuỗi JSON mà producer gửi vào Kafka, rồi đi qua cùng một hàm
parse_events() và cùng luật kiểm tra.

Chạy lại bao nhiêu lần cũng KHÔNG nhân đôi dữ liệu: ghi bằng MERGE INTO theo
transaction_id (chỉ chèn giao dịch chưa có).

Chạy:
    docker compose --profile jobs run --rm backfill                       # cả file
    docker compose --profile jobs run --rm backfill --month 2019-01       # 1 tháng
    docker compose --profile jobs run --rm backfill --limit 10000         # thử nhanh
"""

import argparse
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.pii import load_key
from common.schema import parse_events, sparkov_csv_to_raw
from common.tables import (BRONZE_TABLE, DLQ_TABLE, SILVER_TABLE, create_tables, dedup_batch,
                           invalid_rows, to_silver, valid_rows)
from common.validation import with_error_reason


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Nap lich su giao dich tu CSV Sparkov")
    p.add_argument("--file", default="/data/sparkov/fraudTrain.csv")
    p.add_argument("--month", help="Chi nap mot thang, vi du 2019-01")
    p.add_argument("--limit", type=int, default=0, help="Chi doc N dong dau (de thu)")
    return p.parse_args(argv)


def read_validated_csv(spark, path: str, month=None, limit: int = 0):
    """CSV Sparkov -> DataFrame đã gắn error_reason (giống hệt luồng Kafka)."""
    csv_df = (spark.read
              .option("header", "true")
              .option("multiLine", "false")
              .option("escape", '"')
              .csv(path))
    if limit:
        csv_df = csv_df.limit(limit)
    validated = with_error_reason(parse_events(sparkov_csv_to_raw(csv_df, "batch")))
    if month:
        validated = validated.where(F.date_format("event_time", "yyyy-MM") == month)
    return validated


def merge_new(spark, df, table: str, key_cond: str, view: str) -> None:
    """Chèn các dòng chưa có trong bảng; chạy lại không tạo bản trùng."""
    df.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {table} t
        USING {view} s
        ON {key_cond}
        WHEN NOT MATCHED THEN INSERT *
    """)


def count(spark, table: str) -> int:
    return spark.table(table).count()


def list_months(df) -> list:
    """Các tháng (yyyy-MM) có trong dữ liệu, theo thứ tự thời gian."""
    rows = (df.select(F.date_format("event_time", "yyyy-MM").alias("m"))
              .where(F.col("m").isNotNull()).distinct().orderBy("m").collect())
    return [r.m for r in rows]


def main(argv=None):
    args = parse_args(argv)
    key = load_key()
    spark = SparkSession.builder.appName("batch_backfill").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    started = time.time()
    scope = f"thang {args.month}" if args.month else "ca file"
    print(f"[backfill] Doc {args.file} ({scope})...", flush=True)

    # DISK_ONLY: giữ kết quả đọc CSV trên ổ đĩa thay vì RAM (1,3 triệu dòng không vừa heap 2 GB)
    validated = (read_validated_csv(spark, args.file, args.month, args.limit)
                 .persist(StorageLevel.DISK_ONLY))
    total = validated.count()
    n_bad = validated.where(F.col("error_reason").isNotNull()).count()
    print(f"[backfill] {total:,} dong, trong do {n_bad:,} dong vi pham luat", flush=True)

    before = {t: count(spark, t) for t in (BRONZE_TABLE, DLQ_TABLE, SILVER_TABLE)}

    # Bản ghi lỗi rất ít -> ghi DLQ một lần
    merge_new(spark, invalid_rows(validated).dropDuplicates(["raw_value"]), DLQ_TABLE,
              "t.raw_value = s.raw_value", "src_dlq")

    # Bản ghi hợp lệ -> nạp TỪNG THÁNG để mỗi lần chỉ xử lý ~70 nghìn dòng (không tràn RAM).
    # Bị dừng giữa chừng thì chạy lại: các tháng đã nạp sẽ ra +0 nhờ MERGE.
    valid_all = valid_rows(validated)
    months = list_months(valid_all)
    for i, m in enumerate(months, 1):
        valid = dedup_batch(valid_all.where(F.date_format("event_time", "yyyy-MM") == m))
        merge_new(spark, valid, BRONZE_TABLE, "t.transaction_id = s.transaction_id",
                  "src_bronze")
        merge_new(spark, to_silver(valid, key), SILVER_TABLE,
                  "t.transaction_id = s.transaction_id", "src_silver")
        print(f"[backfill] thang {m} xong ({i}/{len(months)}, "
              f"{time.time() - started:,.0f} giay)", flush=True)

    for t in (BRONZE_TABLE, DLQ_TABLE, SILVER_TABLE):
        after = count(spark, t)
        print(f"[backfill] {t}: +{after - before[t]:,} dong (tong {after:,})", flush=True)
    print(f"[backfill] Xong sau {time.time() - started:,.0f} giay.", flush=True)
    validated.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
