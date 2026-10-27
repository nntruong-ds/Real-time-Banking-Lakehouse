"""Hàm tiện ích cho test: biến sự kiện (dict) thành DataFrame giống hệt khi đọc từ Kafka."""

import json
from datetime import datetime

from common.schema import kafka_to_raw, parse_events
from common.validation import with_error_reason


def kafka_df(spark, events: list):
    now = datetime.now()
    data = [(bytearray(json.dumps(e).encode()), i % 3, i, now) for i, e in enumerate(events)]
    return spark.createDataFrame(
        data, "value BINARY, partition INT, offset BIGINT, timestamp TIMESTAMP")


def validated_from_events(spark, events: list):
    return with_error_reason(parse_events(kafka_to_raw(kafka_df(spark, events))))
