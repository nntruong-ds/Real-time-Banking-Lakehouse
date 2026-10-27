"""
Định nghĩa bảng (DDL) và các phép biến đổi dùng chung giữa job stream và job batch.

    Bronze  lakehouse.bronze.transactions_raw  bản ghi hợp lệ, giữ nguyên trạng (có PII)
    DLQ     lakehouse.bronze.dlq               bản ghi lỗi + lý do
    Silver  lakehouse.silver.transactions      đã bỏ trùng, đã che PII, sẵn sàng phân tích

tests/test_schema.py kiểm tra cột mà job ghi ra khớp đúng thứ tự và kiểu với DDL.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.pii import age_years, haversine_km, hmac_sha256, last4
from common.schema import EVENT_FIELDS, META_COLS

BRONZE_TABLE = "lakehouse.bronze.transactions_raw"
DLQ_TABLE = "lakehouse.bronze.dlq"
SILVER_TABLE = "lakehouse.silver.transactions"

PARQUET_PROPS = """TBLPROPERTIES ('write.format.default'='parquet',
               'write.parquet.compression-codec'='zstd')"""

CREATE_BRONZE = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    transaction_id   STRING,
    card_number      STRING     COMMENT 'PII - chi tang Bronze duoc giu',
    merchant         STRING,
    category         STRING,
    amount           DOUBLE,
    currency         STRING,
    first_name       STRING     COMMENT 'PII',
    last_name        STRING     COMMENT 'PII',
    gender           STRING,
    street           STRING     COMMENT 'PII',
    city             STRING,
    state            STRING,
    zip              STRING     COMMENT 'PII',
    cust_lat         DOUBLE     COMMENT 'PII - toa do nha khach',
    cust_long        DOUBLE     COMMENT 'PII - toa do nha khach',
    city_pop         BIGINT,
    job              STRING,
    dob              DATE       COMMENT 'PII',
    merch_lat        DOUBLE,
    merch_long       DOUBLE,
    event_time       TIMESTAMP  COMMENT 'Thoi diem giao dich xay ra (UTC)',
    is_fraud         INT        COMMENT 'Nhan gian lan cua dataset, dung de danh gia',
    sim_scenario     STRING     COMMENT 'real | invalid (loi cai vao de kiem tra)',
    ingest_source    STRING     COMMENT 'stream (Kafka) | batch (backfill CSV)',
    raw_value        STRING     COMMENT 'Ban ghi JSON goc',
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_timestamp  TIMESTAMP,
    ingest_time      TIMESTAMP  COMMENT 'Thoi diem ghi vao Bronze'
)
USING iceberg
PARTITIONED BY (months(event_time))
{PARQUET_PROPS}
"""

CREATE_DLQ = f"""
CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
    error_reason     STRING     COMMENT 'Ten luat nghiep vu bi vi pham',
    transaction_id   STRING,
    ingest_source    STRING,
    raw_value        STRING     COMMENT 'Ban ghi goc de xu ly lai sau',
    kafka_partition  INT,
    kafka_offset     BIGINT,
    kafka_timestamp  TIMESTAMP,
    ingest_time      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingest_time))
"""

CREATE_SILVER = f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    transaction_id   STRING,
    card_token       STRING     COMMENT 'HMAC-SHA256 cua so the',
    card_last4       STRING,
    gender           STRING,
    age              INT        COMMENT 'Tuoi luc giao dich (thay cho ngay sinh)',
    city             STRING,
    state            STRING,
    city_pop         BIGINT,
    merchant         STRING     COMMENT 'Da bo tien to fraud_ cua dataset',
    category         STRING,
    amount           DOUBLE,
    currency         STRING,
    merch_lat        DOUBLE,
    merch_long       DOUBLE,
    distance_km      DOUBLE     COMMENT 'Khoang cach nha khach - cua hang',
    event_time       TIMESTAMP,
    is_fraud         INT,
    ingest_source    STRING,
    ingest_time      TIMESTAMP,
    silver_time      TIMESTAMP
)
USING iceberg
PARTITIONED BY (months(event_time))
{PARQUET_PROPS}
"""

BRONZE_COLS = EVENT_FIELDS + META_COLS
DLQ_COLS = ["error_reason", "transaction_id", "ingest_source", "raw_value",
            "kafka_partition", "kafka_offset", "kafka_timestamp", "ingest_time"]


def create_tables(spark) -> None:
    for ns in ("lakehouse.bronze", "lakehouse.silver"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    for ddl in (CREATE_BRONZE, CREATE_DLQ, CREATE_SILVER):
        spark.sql(ddl)
    evolve_partitioning(spark)


def _partitioning(spark, table: str) -> list:
    """Cách chia partition HIỆN TẠI của bảng, ví dụ ['months(event_time)']."""
    rows = spark.sql(f"DESCRIBE TABLE {table}").collect()
    return [r.data_type for r in rows if (r.col_name or "").startswith("Part ")]


def evolve_partitioning(spark) -> None:
    """Bảng tạo từ phiên bản cũ chia theo NGÀY -> đổi sang chia theo THÁNG.

    Vì sao: dataset chỉ ~3 nghìn giao dịch/ngày, và producer tua nhanh 3600 lần nên một mẻ
    stream trải hàng chục ngày. Chia theo ngày làm mỗi mẻ phải mở hàng chục file Parquet cùng
    lúc (tràn heap) và sinh rất nhiều file nhỏ. Iceberg cho đổi cách chia mà không ghi lại dữ
    liệu cũ (partition evolution): dữ liệu cũ giữ nguyên, dữ liệu mới ghi theo tháng.
    """
    for table in (BRONZE_TABLE, SILVER_TABLE):
        spec = _partitioning(spark, table)
        print(f"[tables] {table}: partition hien tai = {spec}", flush=True)
        if any("month" in s for s in spec):
            continue
        try:
            spark.sql(f"ALTER TABLE {table} REPLACE PARTITION FIELD event_time_day "
                      f"WITH months(event_time)")
            print(f"[tables] {table}: doi partition ngay -> thang. "
                  f"Bay gio = {_partitioning(spark, table)}", flush=True)
        except Exception as e:
            # Job còn lại (bronze/silver) có thể vừa đổi xong cùng lúc -> chỉ báo nếu vẫn chưa đổi
            if not any("month" in s for s in _partitioning(spark, table)):
                print(f"[tables] {table}: KHONG doi duoc partition: "
                      f"{str(e).splitlines()[0]}", flush=True)


def valid_rows(validated: DataFrame) -> DataFrame:
    return validated.where(F.col("error_reason").isNull()).select(*BRONZE_COLS)


def invalid_rows(validated: DataFrame) -> DataFrame:
    return validated.where(F.col("error_reason").isNotNull()).select(*DLQ_COLS)


def dedup_stream(df: DataFrame, watermark: str) -> DataFrame:
    """Bỏ trùng theo transaction_id, nhớ các mã đã thấy trong `watermark` (thời gian giao dịch)."""
    return (df.where(F.col("transaction_id").isNotNull())
              .withWatermark("event_time", watermark)
              .dropDuplicatesWithinWatermark(["transaction_id"]))


def dedup_batch(df: DataFrame) -> DataFrame:
    return df.where(F.col("transaction_id").isNotNull()).dropDuplicates(["transaction_id"])


def to_silver(df: DataFrame, key: bytes) -> DataFrame:
    """Cột Bronze -> cột Silver: che PII, chuẩn hóa. Thứ tự cột phải giống CREATE_SILVER."""
    return df.select(
        "transaction_id",
        hmac_sha256(F.col("card_number"), key).alias("card_token"),
        last4(F.col("card_number")).alias("card_last4"),
        F.upper(F.trim("gender")).alias("gender"),
        age_years(F.col("dob"), F.col("event_time")).alias("age"),
        F.trim("city").alias("city"),
        F.upper(F.trim("state")).alias("state"),
        "city_pop",
        F.regexp_replace(F.trim("merchant"), "^fraud_", "").alias("merchant"),
        F.trim("category").alias("category"),
        "amount",
        F.upper(F.trim("currency")).alias("currency"),
        "merch_lat",
        "merch_long",
        F.round(haversine_km(F.col("cust_lat"), F.col("cust_long"),
                             F.col("merch_lat"), F.col("merch_long")), 2).alias("distance_km"),
        "event_time",
        "is_fraud",
        "ingest_source",
        "ingest_time",
        F.current_timestamp().alias("silver_time"),
    )
