"""
Cấu trúc sự kiện giao dịch dùng chung cho MỌI job (stream lẫn batch).

Mỗi giao dịch, dù đến từ Kafka (producer phát lại) hay từ file CSV (backfill),
đều được đưa về cùng một chuỗi JSON "chuẩn" trong cột raw_value, rồi đi qua
CÙNG MỘT hàm parse_events(). Nhờ vậy hai đường nạp không thể lệch nhau về kiểu
dữ liệu hay luật kiểm tra.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Kiểu dữ liệu của từng trường TRONG JSON (thời gian và ngày sinh nhận dạng chuỗi,
# rồi chuyển kiểu an toàn bên dưới).
EVENT_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("card_number", T.StringType()),
    T.StructField("merchant", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("first_name", T.StringType()),
    T.StructField("last_name", T.StringType()),
    T.StructField("gender", T.StringType()),
    T.StructField("street", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("zip", T.StringType()),
    T.StructField("cust_lat", T.DoubleType()),
    T.StructField("cust_long", T.DoubleType()),
    T.StructField("city_pop", T.LongType()),
    T.StructField("job", T.StringType()),
    T.StructField("dob", T.StringType()),
    T.StructField("merch_lat", T.DoubleType()),
    T.StructField("merch_long", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("is_fraud", T.IntegerType()),
    T.StructField("sim_scenario", T.StringType()),
])
EVENT_FIELDS = [f.name for f in EVENT_SCHEMA.fields]

# Cột "siêu dữ liệu" đi kèm mỗi bản ghi
META_COLS = ["ingest_source", "raw_value", "kafka_partition", "kafka_offset",
             "kafka_timestamp", "ingest_time"]


def safe_cast(sql_expr: str, sql_type: str) -> Column:
    """Chuyển kiểu; sai định dạng thì ra NULL thay vì làm sập job.

    Spark 4 bật ANSI mode mặc định: cast('abc' AS TIMESTAMP) sẽ ném lỗi và dừng cả
    luồng streaming. try_cast trả về NULL, rồi luật kiểm tra đưa bản ghi sang DLQ.
    """
    return F.expr(f"try_cast({sql_expr} AS {sql_type})")


def safe_timestamp(sql_expr: str) -> Column:
    return safe_cast(sql_expr, "TIMESTAMP")


def parsed_ok(struct_col: str, fields: list) -> Column:
    """True nếu JSON giải mã được (ít nhất một trường có giá trị)."""
    c = F.col(struct_col)
    any_field = F.coalesce(*[c[f].cast("string") for f in fields]).isNotNull()
    return c.isNotNull() & any_field


def parse_events(df: DataFrame) -> DataFrame:
    """raw_value (JSON) + cột meta  ->  cột sự kiện đã chuyển kiểu + _parsed_ok + meta.

    Thứ tự cột kết quả: _parsed_ok, EVENT_FIELDS..., META_COLS...
    """
    d = "_evt"
    typed = []
    for f in EVENT_FIELDS:
        if f == "event_time":
            typed.append(safe_timestamp(f"{d}.event_time").alias(f))
        elif f == "dob":
            typed.append(safe_cast(f"{d}.dob", "DATE").alias(f))
        else:
            typed.append(F.col(f"{d}.{f}").alias(f))
    return (df.withColumn(d, F.from_json("raw_value", EVENT_SCHEMA))
              .select(parsed_ok(d, EVENT_FIELDS).alias("_parsed_ok"), *typed, *META_COLS))


def kafka_to_raw(kafka_df: DataFrame, source: str = "stream") -> DataFrame:
    """DataFrame đọc từ Kafka -> raw_value + cột meta."""
    return kafka_df.select(
        F.lit(source).alias("ingest_source"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
    )


def sparkov_csv_to_raw(csv_df: DataFrame, source: str = "batch") -> DataFrame:
    """DataFrame đọc từ file CSV Sparkov (mọi cột là chuỗi) -> raw_value + cột meta.

    Ánh xạ cột GIỐNG HỆT producer phát lại (data_generator/replay.py: row_to_event);
    tests/test_backfill.py kiểm tra hai bên cho ra cùng kết quả.
    """
    event = F.struct(
        F.col("trans_num").alias("transaction_id"),
        F.col("cc_num").alias("card_number"),
        F.col("merchant"),
        F.col("category"),
        safe_cast("amt", "DOUBLE").alias("amount"),
        F.lit("USD").alias("currency"),
        F.col("first").alias("first_name"),
        F.col("last").alias("last_name"),
        F.col("gender"),
        F.col("street"),
        F.col("city"),
        F.col("state"),
        F.col("zip"),
        safe_cast("lat", "DOUBLE").alias("cust_lat"),
        safe_cast("`long`", "DOUBLE").alias("cust_long"),
        safe_cast("city_pop", "BIGINT").alias("city_pop"),
        F.col("job"),
        F.col("dob"),
        safe_cast("merch_lat", "DOUBLE").alias("merch_lat"),
        safe_cast("merch_long", "DOUBLE").alias("merch_long"),
        F.concat(F.regexp_replace("trans_date_trans_time", " ", "T"), F.lit("Z"))
         .alias("event_time"),
        safe_cast("is_fraud", "INT").alias("is_fraud"),
        F.lit("real").alias("sim_scenario"),
    )
    return csv_df.select(
        F.lit(source).alias("ingest_source"),
        F.to_json(event).alias("raw_value"),
        F.lit(None).cast("int").alias("kafka_partition"),
        F.lit(None).cast("bigint").alias("kafka_offset"),
        F.lit(None).cast("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
    )
