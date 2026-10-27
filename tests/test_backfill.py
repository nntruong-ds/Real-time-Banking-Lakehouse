"""
Kiểm tra job batch backfill: đọc CSV Sparkov phải cho kết quả GIỐNG HỆT đường Kafka.

Nếu ánh xạ cột trong Spark (common/schema.py: sparkov_csv_to_raw) và trong producer
(replay.py: row_to_event) lệch nhau, dữ liệu lịch sử và dữ liệu realtime sẽ khác nhau
một cách âm thầm. Bài test này bắt lỗi đó.
"""

from pyspark.sql import functions as F

from batch_backfill import list_months, read_validated_csv
from common.schema import EVENT_FIELDS
from common.tables import dedup_batch, to_silver, valid_rows
from helpers import validated_from_events
from replay import row_to_event
from sample_data import make_rows, write_csv


def test_csv_path_equals_kafka_path(spark, tmp_path):
    rows = make_rows(120, seed=5)
    path = str(tmp_path / "sample.csv")
    write_csv(rows, path)

    batch = {r.transaction_id: r for r in read_validated_csv(spark, path).collect()}
    stream = {r.transaction_id: r for r in
              validated_from_events(spark, [row_to_event(r) for r in rows]).collect()}

    assert set(batch) == set(stream) == {r["trans_num"] for r in rows}
    for tid, b in batch.items():
        s = stream[tid]
        for f in EVENT_FIELDS + ["error_reason"]:
            assert b[f] == s[f], f"{tid}.{f}: batch={b[f]!r} stream={s[f]!r}"
        assert b.ingest_source == "batch" and s.ingest_source == "stream"


def test_month_filter_and_silver(spark, key, tmp_path):
    rows = make_rows(400, seed=9, start="2019-01-30 00:00:00")
    rows.append(dict(rows[0]))                      # một dòng trùng trong file
    path = str(tmp_path / "sample.csv")
    write_csv(rows, path)

    jan = read_validated_csv(spark, path, month="2019-01")
    months = {r[0] for r in jan.selectExpr("date_format(event_time, 'yyyy-MM')").collect()}
    assert months == {"2019-01"}

    silver = to_silver(dedup_batch(valid_rows(read_validated_csv(spark, path))), key)
    assert silver.count() == len({r["trans_num"] for r in rows})
    assert silver.where("card_token IS NULL OR merchant LIKE 'fraud_%'").count() == 0


def test_month_by_month_covers_everything(spark, tmp_path):
    """Nạp từng tháng phải ra đúng số dòng như nạp một lần (không sót, không trùng)."""
    rows = make_rows(400, seed=9, start="2019-01-31 21:00:00")
    path = str(tmp_path / "sample.csv")
    write_csv(rows, path)

    valid_all = valid_rows(read_validated_csv(spark, path))
    months = list_months(valid_all)
    assert months == sorted(months) and len(months) >= 2
    per_month = sum(
        dedup_batch(valid_all.where(F.date_format("event_time", "yyyy-MM") == m)).count()
        for m in months)
    assert per_month == dedup_batch(valid_all).count()
