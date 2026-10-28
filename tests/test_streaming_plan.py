"""
Kiểm tra kế hoạch truy vấn STREAMING của Bronze lập được mà không tràn bộ nhớ.

Lỗi từng gặp: query bronze_valid tràn heap ngay lúc Spark lập kế hoạch (chưa đọc dòng dữ liệu
nào), vì bộ tối ưu "constraint propagation" bùng nổ trên bộ lọc error_reason IS NULL.
Cách sửa: spark.sql.constraintPropagation.enabled = false trong spark-defaults.conf.
Test batch không bắt được lỗi này -> phải chạy một query streaming thật (nguồn "rate"
giả làm Kafka, ghi ra sink "noop").
"""

import json
import os

from pyspark.sql import functions as F

from common.schema import kafka_to_raw, parse_events
from common.tables import invalid_rows, valid_rows
from common.validation import with_error_reason
from replay import row_to_event
from sample_data import make_rows

# Chạy trên máy/CI: file trong repo. Chạy trong container spark-tools: file đã gắn vào Spark.
REPO = os.path.join(os.path.dirname(__file__), "..")
CANDIDATES = [os.path.join(REPO, "docker", "spark", "spark-defaults.conf"),
              "/opt/spark/conf/spark-defaults.conf"]


def test_defaults_disable_constraint_propagation():
    path = next(p for p in CANDIDATES if os.path.exists(p))
    lines = [ln.split() for ln in open(path, encoding="utf-8")
             if ln.strip() and not ln.startswith("#")]
    assert ["spark.sql.constraintPropagation.enabled", "false"] in lines


def test_streaming_bronze_plan_runs(spark):
    event = json.dumps(row_to_event(make_rows(1)[0]))
    fake_kafka = (spark.readStream.format("rate").option("rowsPerSecond", 20).load()
                  .select(F.lit(None).cast("binary").alias("key"),
                          F.lit(event).cast("binary").alias("value"),
                          F.lit("transactions").alias("topic"),
                          F.lit(0).alias("partition"),
                          F.col("value").alias("offset"),
                          F.col("timestamp")))
    validated = with_error_reason(parse_events(kafka_to_raw(fake_kafka, "stream")))

    for name, df in (("valid", valid_rows(validated)), ("invalid", invalid_rows(validated))):
        q = (df.writeStream.format("noop").queryName(f"plan_{name}")
               .trigger(processingTime="1 second").start())
        try:
            q.awaitTermination(8)
            assert q.exception() is None, q.exception()
            assert q.lastProgress is not None and q.lastProgress["batchId"] >= 1, name
        finally:
            q.stop()
