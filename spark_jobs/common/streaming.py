"""Phần dùng chung của hai job streaming: đọc Kafka đã kiểm tra, và vòng lặp in tiến độ."""

import os
import time

from common.schema import kafka_to_raw, parse_events
from common.validation import with_error_reason

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
SOURCE_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq_transactions")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/spark/checkpoints")
# Số tin tối đa mỗi mẻ. Lúc chạy đều chỉ ~3-5 nghìn tin/mẻ; giới hạn này chỉ có tác dụng khi
# Kafka bị dồn tin (job tắt một lúc): chia phần dồn thành nhiều mẻ nhỏ để không tràn heap.
MAX_OFFSETS = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "10000"))


def read_validated_kafka(spark):
    """Đọc topic giao dịch, giải mã JSON, gắn cột error_reason (NULL = hợp lệ)."""
    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .option("maxOffsetsPerTrigger", MAX_OFFSETS)
                .option("failOnDataLoss", "false")
                .load())
    return with_error_reason(parse_events(kafka_to_raw(kafka_df, "stream")))


def checkpoint(name: str) -> str:
    return os.path.join(CHECKPOINT_ROOT, name)


def run_until_stopped(queries: list, tag: str) -> None:
    """In tiến độ mỗi mẻ; một query lỗi thì dừng tất cả để Docker khởi động lại job."""
    last = {}
    try:
        while all(q.isActive for q in queries):
            for q in queries:
                p = q.lastProgress
                if p and last.get(q.name) != p["batchId"]:
                    last[q.name] = p["batchId"]
                    print(f"[{tag}] {q.name:<12} me #{p['batchId']}: "
                          f"doc {p['numInputRows']:,} ban ghi tu Kafka", flush=True)
            time.sleep(5)
    except KeyboardInterrupt:
        print(f"[{tag}] Dang dung...", flush=True)

    for q in queries:
        if q.isActive:
            q.stop()
    for q in queries:
        if q.exception():
            print(f"[{tag}] Query {q.name} bi loi:", flush=True)
            raise q.exception()
