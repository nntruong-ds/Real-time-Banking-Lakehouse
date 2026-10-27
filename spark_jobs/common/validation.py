"""
Luật nghiệp vụ (business rules) quyết định bản ghi nào hợp lệ.

Bản ghi vi phạm được gắn cột `error_reason` và đưa sang DLQ (Dead Letter Queue)
thay vì vào Bronze/Silver, nhờ vậy luồng chính không bị dừng.

Chỉ dùng biểu thức có sẵn của Spark (không dùng Python UDF) nên chạy nhanh.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_CURRENCIES = ["USD"]

# Giao dịch có thời gian ở tương lai quá ngưỡng này bị coi là sai
MAX_FUTURE_MINUTES = 5


def _blank(c: str) -> Column:
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def _bad_coord(lat: str, lon: str) -> Column:
    return (F.col(lat).isNull() | F.col(lon).isNull()
            | (F.abs(F.col(lat)) > 90) | (F.abs(F.col(lon)) > 180))


def _rules() -> list:
    """Danh sách (tên lỗi, điều kiện LỖI). Lỗi đứng trước được ghi nhận trước."""
    return [
        ("malformed_json", ~F.col("_parsed_ok")),
        ("missing_transaction_id", _blank("transaction_id")),
        ("missing_card_number", _blank("card_number")),
        ("missing_merchant", _blank("merchant")),
        ("missing_amount", F.col("amount").isNull()),
        ("non_positive_amount", F.col("amount") <= 0),
        ("unknown_currency",
         F.col("currency").isNull() | ~F.col("currency").isin(VALID_CURRENCIES)),
        ("invalid_event_time", F.col("event_time").isNull()),
        ("future_event_time",
         F.col("event_time") > F.current_timestamp()
         + F.expr(f"INTERVAL {MAX_FUTURE_MINUTES} MINUTES")),
        ("invalid_coordinates",
         _bad_coord("merch_lat", "merch_long") | _bad_coord("cust_lat", "cust_long")),
    ]


def with_error_reason(df: DataFrame) -> DataFrame:
    """Thêm cột error_reason: NULL nếu hợp lệ, ngược lại là tên lỗi đầu tiên gặp."""
    reason = F.lit(None).cast("string")
    for name, failed in reversed(_rules()):
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    return df.withColumn("error_reason", reason)
