"""
Spark job bảo trì Iceberg — gộp file nhỏ (compaction) và ĐO hiệu quả trước/sau.

Vì sao cần: job streaming ghi một commit mỗi 30 giây, mỗi commit sinh vài file Parquet nhỏ.
Sau vài giờ, một bảng có hàng nghìn file vài trăm KB. Mỗi truy vấn phải mở từng file
(mỗi file = vài lượt gọi tới kho S3), nên càng nhiều file càng chậm: "small files problem".

Job này, với từng bảng:
  1. Đo:  số file, dung lượng, thời gian một truy vấn quét toàn bảng (trung vị 3 lần).
  2. rewrite_data_files  : gộp file nhỏ thành file ~TARGET_FILE_MB (mặc định 128 MB), đồng
                            thời chuyển file cũ (partition theo ngày) sang partition theo tháng.
                            Mỗi bảng MỘT commit (tắt partial-progress), lần lượt từng nhóm file.
                            Lý do: metadata Nessie trả về chỉ chứa snapshot HIỆN TẠI. Khi commit
                            từng phần, commit sau phải kiểm tra lịch sử tính từ snapshot lúc bắt
                            đầu, mà snapshot đó đã không còn trong metadata -> "Cannot determine
                            history" / NullPointerException. Cùng lý do: phải TẮT luồng streaming
                            trước khi bảo trì (một commit của stream chen vào giữa cũng gây lỗi).
  3. rewrite_manifests   : gộp các file manifest (danh mục file) để lập kế hoạch truy vấn nhanh hơn.
  4. Đo lại, ghi kết quả vào lakehouse.ops.maintenance_log.

Không chạy expire_snapshots / remove_orphan_files: với Nessie, việc dọn snapshot và file thừa
do Nessie GC đảm nhiệm (Nessie biết file nào còn được branch/tag khác dùng; hai thủ tục
kia của Iceberg thì không).

Chạy:   docker compose --profile jobs run --rm maintenance
        (hoặc DAG lakehouse_maintenance trong Airflow)
"""

import os
import statistics
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

TABLES = [
    "lakehouse.bronze.transactions_raw",
    "lakehouse.bronze.dlq",
    "lakehouse.silver.transactions",
    "lakehouse.gold.transaction_features",
    "lakehouse.gold.fraud_alerts",
]
LOG_TABLE = "lakehouse.ops.maintenance_log"
TARGET_FILE_MB = int(os.getenv("TARGET_FILE_MB", "128"))
BENCH_RUNS = 3

LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("run_time", T.TimestampType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("files_before", T.LongType()),
    T.StructField("files_after", T.LongType()),
    T.StructField("mb_before", T.DoubleType()),
    T.StructField("mb_after", T.DoubleType()),
    T.StructField("query_sec_before", T.DoubleType()),
    T.StructField("query_sec_after", T.DoubleType()),
    T.StructField("rewritten_files", T.LongType()),
    T.StructField("added_files", T.LongType()),
])

CREATE_LOG = f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
    run_id STRING, run_time TIMESTAMP, table_name STRING,
    files_before BIGINT, files_after BIGINT, mb_before DOUBLE, mb_after DOUBLE,
    query_sec_before DOUBLE, query_sec_after DOUBLE, rewritten_files BIGINT, added_files BIGINT
) USING iceberg
"""


def log(msg: str) -> None:
    print(f"[maintenance] {msg}", flush=True)


# ------------------------------------------------------------- hàm thuần (có test) ---
def short_name(table: str) -> str:
    """'lakehouse.bronze.dlq' -> 'bronze.dlq' (tên mà thủ tục CALL của catalog cần)."""
    return table.split(".", 1)[1]


def pct_less(before: float, after: float) -> float:
    """Giảm bao nhiêu phần trăm (âm nếu tăng)."""
    return round(100.0 * (before - after) / before, 1) if before else 0.0


def speedup(before_sec: float, after_sec: float) -> float:
    """Nhanh hơn bao nhiêu lần."""
    return round(before_sec / after_sec, 1) if after_sec else 0.0


def median_seconds(fn, runs: int = BENCH_RUNS) -> float:
    times = []
    for _ in range(runs):
        started = time.perf_counter()
        fn()
        times.append(time.perf_counter() - started)
    return round(statistics.median(times), 2)


# ------------------------------------------------------------- thao tác trên bảng ----
def file_stats(spark, table: str) -> tuple:
    r = spark.sql(f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes "
                  f"FROM {table}.files").first()
    return int(r["files"]), round(int(r["bytes"]) / 1024 / 1024, 1)


def bench(spark, table: str) -> float:
    """Thời gian một truy vấn phải đọc mọi file của bảng."""
    return median_seconds(
        lambda: spark.sql(f"SELECT count(DISTINCT transaction_id) FROM {table}").collect())


def compact(spark, table: str) -> tuple:
    target = TARGET_FILE_MB * 1024 * 1024
    r = spark.sql(f"""
        CALL lakehouse.system.rewrite_data_files(
            table => '{short_name(table)}',
            options => map('target-file-size-bytes', '{target}',
                           'min-input-files', '2',
                           'max-concurrent-file-group-rewrites', '1'))""").first()
    spark.sql(f"CALL lakehouse.system.rewrite_manifests('{short_name(table)}')").collect()
    return int(r["rewritten_data_files_count"]), int(r["added_data_files_count"])


def main():
    started = time.time()
    run_id = str(uuid.uuid4())
    spark = SparkSession.builder.appName("iceberg_maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    spark.sql(CREATE_LOG)

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    errors = []
    for table in TABLES:
        if not spark.catalog.tableExists(table):
            log(f"{table}: chua co bang, bo qua")
            continue
        try:
            files_before, mb_before = file_stats(spark, table)
            sec_before = bench(spark, table)
            log(f"{table}: {files_before:,} file, {mb_before:,} MB, truy van {sec_before} s "
                "-> dang gop file...")
            rewritten, added = compact(spark, table)
            files_after, mb_after = file_stats(spark, table)
            sec_after = bench(spark, table)
        except Exception as e:  # một bảng lỗi không chặn các bảng còn lại
            first = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
            log(f"LOI o bang {table}: {type(e).__name__}: {first[:400]}")
            errors.append(table)
            continue
        log(f"{table}: gop {rewritten:,} file thanh {added:,} file -> con {files_after:,} file "
            f"(giam {pct_less(files_before, files_after)}%), truy van {sec_after} s "
            f"(nhanh x{speedup(sec_before, sec_after)})")
        rows.append((run_id, now, table, files_before, files_after, mb_before, mb_after,
                     sec_before, sec_after, rewritten, added))

    if rows:
        spark.createDataFrame(rows, LOG_SCHEMA).writeTo(LOG_TABLE).append()

    log("=" * 72)
    log(f"{'bang':<36} {'file truoc':>10} {'file sau':>9} {'truy van truoc':>15} {'sau':>6}")
    for r in rows:
        log(f"{short_name(r[2]):<36} {r[3]:>10,} {r[4]:>9,} {r[7]:>13} s {r[8]:>4} s")
    log(f"Da ghi {LOG_TABLE} (run_id {run_id[:8]}). Xong sau {time.time() - started:,.0f} giay.")
    spark.stop()
    if errors:
        # thoát mã lỗi để Airflow / người chạy biết có bảng chưa được bảo trì
        raise SystemExit(f"[maintenance] {len(errors)} bang bi loi: {', '.join(errors)}")


if __name__ == "__main__":
    main()
