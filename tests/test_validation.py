"""Kiểm tra từng luật nghiệp vụ dùng để tách DLQ. Chạy:  pytest -q"""

from datetime import datetime, timedelta, timezone

from helpers import validated_from_events
from replay import row_to_event
from sample_data import make_rows


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


GOOD = row_to_event(make_rows(1)[0])
FUTURE = iso(datetime.now(timezone.utc) + timedelta(days=3))
SOON = iso(datetime.now(timezone.utc) + timedelta(minutes=1))

CASES = [
    ("valid", GOOD, None),
    ("slightly_future_is_ok", dict(GOOD, event_time=SOON), None),
    ("missing_id", dict(GOOD, transaction_id=None), "missing_transaction_id"),
    ("blank_id", dict(GOOD, transaction_id="  "), "missing_transaction_id"),
    ("missing_card", dict(GOOD, card_number=None), "missing_card_number"),
    ("missing_merchant", dict(GOOD, merchant=""), "missing_merchant"),
    ("missing_amount", dict(GOOD, amount=None), "missing_amount"),
    ("amount_not_number", dict(GOOD, amount="abc"), "missing_amount"),
    ("negative", dict(GOOD, amount=-5), "non_positive_amount"),
    ("zero", dict(GOOD, amount=0), "non_positive_amount"),
    ("currency", dict(GOOD, currency="XXX"), "unknown_currency"),
    ("bad_time", dict(GOOD, event_time="not-a-time"), "invalid_event_time"),
    ("future", dict(GOOD, event_time=FUTURE), "future_event_time"),
    ("bad_merchant_lat", dict(GOOD, merch_lat=999.0), "invalid_coordinates"),
    ("missing_customer_long", dict(GOOD, cust_long=None), "invalid_coordinates"),
]


def test_each_rule(spark):
    events = [dict(e, sim_scenario=name) for name, e, _ in CASES]
    got = {r.sim_scenario: r.error_reason
           for r in validated_from_events(spark, events).collect()}
    for name, _, expected in CASES:
        assert got[name] == expected, f"{name}: {got[name]} != {expected}"


def test_not_json_and_empty_object(spark):
    from datetime import datetime as dt
    data = [(bytearray(b"this is not json"), 0, 0, dt.now()), (bytearray(b"{}"), 0, 1, dt.now())]
    df = spark.createDataFrame(
        data, "value BINARY, partition INT, offset BIGINT, timestamp TIMESTAMP")
    from common.schema import kafka_to_raw, parse_events
    from common.validation import with_error_reason
    reasons = [r.error_reason for r in with_error_reason(parse_events(kafka_to_raw(df))).collect()]
    assert reasons == ["malformed_json", "malformed_json"]
