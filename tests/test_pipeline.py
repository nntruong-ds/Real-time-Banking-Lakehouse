"""
Kiểm tra cả luồng streaming với dữ liệu định dạng Sparkov (không cần Kafka):
  CSV mẫu -> producer phát lại (kèm lỗi + trùng cài sẵn) -> kiểm tra -> Silver
Chạy:  pytest -q
"""

import hashlib
import hmac
import random
from collections import Counter

import pytest
from pyspark.sql import functions as F

import replay
from common.tables import dedup_stream, invalid_rows, to_silver, valid_rows
from helpers import validated_from_events
from sample_data import make_rows

# Lỗi producer cố tình tạo  ->  luật phải bắt được
EXPECTED_RULE = {
    "negative_amount": "non_positive_amount",
    "zero_amount": "non_positive_amount",
    "unknown_currency": "unknown_currency",
    "future_time": "future_event_time",
    "missing_id": "missing_transaction_id",
    "bad_coordinates": "invalid_coordinates",
}
PII_COLUMNS = {"card_number", "first_name", "last_name", "street", "zip", "dob",
               "cust_lat", "cust_long", "job"}


@pytest.fixture(scope="module")
def replayed(monkeypatch_module):
    rows = make_rows(600, seed=11)
    rng = random.Random(3)
    monkeypatch_module.setattr(replay, "INVALID_RATIO", 0.05)
    monkeypatch_module.setattr(replay, "DUPLICATE_RATIO", 0.05)
    events = []
    for r in rows:
        events += replay.with_faults(replay.row_to_event(r), rng)
    return rows, events


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_every_injected_error_goes_to_dlq(spark, replayed):
    _, events = replayed
    out = validated_from_events(spark, events).collect()
    assert len(out) == len(events)
    kinds = Counter()
    for e, r in zip(events, sorted(out, key=lambda r: r.kafka_offset)):
        if e["sim_scenario"] == "invalid":
            kinds[e["sim_invalid_reason"]] += 1
            assert r.error_reason == EXPECTED_RULE[e["sim_invalid_reason"]], e
        else:
            assert r.error_reason is None, (e, r.error_reason)
    assert len(kinds) >= 3, "can co nhieu loai loi khac nhau trong mau"


def test_dlq_keeps_original_record(spark, replayed):
    _, events = replayed
    dlq = invalid_rows(validated_from_events(spark, events))
    assert dlq.count() == sum(e["sim_scenario"] == "invalid" for e in events)
    assert dlq.where(F.col("raw_value").isNull() | F.col("error_reason").isNull()).count() == 0


def test_silver_dedups_and_protects_pii(spark, key, tmp_path, replayed):
    rows, events = replayed
    bronze = valid_rows(validated_from_events(spark, events))
    real_ids = [r["trans_num"] for r in rows]
    assert bronze.count() > len(real_ids), "Bronze phai con ban trung"

    src = str(tmp_path / "bronze")
    bronze.write.parquet(src)
    stream = spark.readStream.schema(bronze.schema).parquet(src)
    q = (to_silver(dedup_stream(stream, "2 hours"), key).writeStream
         .format("memory").queryName("silver_test")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    silver = spark.table("silver_test")

    # 1. Đủ mọi giao dịch thật, không thừa bản trùng
    assert silver.count() == len(set(real_ids))
    assert {r.transaction_id for r in silver.select("transaction_id").collect()} == set(real_ids)

    # 2. Không còn cột PII
    assert not PII_COLUMNS & set(silver.columns)

    # 3. Token đúng chuẩn HMAC-SHA256; merchant đã bỏ tiền tố "fraud_"
    by_id = {r["trans_num"]: r for r in rows}
    for s in silver.limit(50).collect():
        src_row = by_id[s.transaction_id]
        assert s.card_token == hmac.new(key, src_row["cc_num"].encode(),
                                        hashlib.sha256).hexdigest()
        assert s.card_last4 == src_row["cc_num"][-4:]
        assert not s.merchant.startswith("fraud_")
        assert s.age is not None and 15 < s.age < 90
        assert s.distance_km is not None and s.distance_km >= 0
        assert s.is_fraud == int(src_row["is_fraud"])

    # 4. Cùng thẻ -> cùng token
    assert silver.select("card_token").distinct().count() == len({r["cc_num"] for r in rows})
