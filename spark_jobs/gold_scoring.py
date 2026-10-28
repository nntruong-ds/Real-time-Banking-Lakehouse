"""
Spark job Gold — đặc trưng theo thẻ + chấm điểm gian lận (scorecard WoE) + đo kết quả.

    lakehouse.silver.transactions
        │  card_features(): đếm/tổng/trung bình theo thẻ, chỉ nhìn về quá khứ
        ▼
    chia TRAIN (trước SPLIT_TIME) / TEST (từ SPLIT_TIME)
        │  train_scorecard(): học WoE trên TRAIN
        │  apply_scorecard(): điểm + lý do cho MỌI giao dịch
        │  chọn ngưỡng trên TRAIN (F1 cao nhất), đo precision/recall/AUC trên TEST
        ▼
    lakehouse.gold.transaction_features   mọi giao dịch: đặc trưng, điểm, lý do, có cảnh báo không
    lakehouse.gold.scorecard              bảng điểm: (đặc trưng, ô) -> WoE, IV
    lakehouse.gold.fraud_alerts           giao dịch bị cảnh báo (hàng đợi cho người duyệt)
    lakehouse.gold.model_metrics          kết quả đánh giá, MỖI lần chạy thêm dòng mới

Chạy lại bao nhiêu lần cũng được: dữ liệu 3 bảng đầu được thay nguyên khối trong một commit
(atomic), bảng model_metrics giữ lịch sử để so sánh các lần chạy.

Chạy:   docker compose --profile jobs run --rm gold
"""

import os
import time
import uuid
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.gold import (ALERT_RATES, FEATURE_COLS, FEATURES, add_bins, apply_scorecard,
                         card_features, choose_threshold, evaluate, information_values,
                         roc_auc, thresholds_for_rates, train_scorecard, with_split)
from common.tables import SILVER_TABLE

GOLD_NS = "lakehouse.gold"
FEATURES_TABLE = f"{GOLD_NS}.transaction_features"
SCORECARD_TABLE = f"{GOLD_NS}.scorecard"
ALERTS_TABLE = f"{GOLD_NS}.fraud_alerts"
METRICS_TABLE = f"{GOLD_NS}.model_metrics"

# Ranh giới giữa fraudTrain.csv (kết thúc 21/06/2020 12:13) và fraudTest.csv (bắt đầu 12:14)
SPLIT_TIME = os.getenv("SPLIT_TIME", "2020-06-21 12:14:00")

METRICS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("run_time", T.TimestampType()),
    T.StructField("split", T.StringType()),
    T.StructField("target_alert_rate", T.DoubleType()),
    T.StructField("threshold", T.DoubleType()),
    T.StructField("n", T.LongType()),
    T.StructField("alerts", T.LongType()),
    T.StructField("alert_rate", T.DoubleType()),
    T.StructField("tp", T.LongType()),
    T.StructField("fp", T.LongType()),
    T.StructField("fn", T.LongType()),
    T.StructField("precision", T.DoubleType()),
    T.StructField("recall", T.DoubleType()),
    T.StructField("f1", T.DoubleType()),
    T.StructField("roc_auc", T.DoubleType()),
    T.StructField("is_chosen", T.BooleanType()),
])

CREATE_METRICS = f"""
CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
    run_id STRING, run_time TIMESTAMP, split STRING, target_alert_rate DOUBLE,
    threshold DOUBLE, n BIGINT, alerts BIGINT, alert_rate DOUBLE,
    tp BIGINT, fp BIGINT, fn BIGINT, precision DOUBLE, recall DOUBLE, f1 DOUBLE,
    roc_auc DOUBLE, is_chosen BOOLEAN
) USING iceberg
"""


def log(msg: str) -> None:
    print(f"[gold] {msg}", flush=True)


def replace_table(spark, df, table: str, partition_by_month: bool = True) -> None:
    """Thay toàn bộ dữ liệu bảng trong MỘT commit Iceberg (không ai thấy bảng dở dang).

    Lần đầu: tạo bảng. Các lần sau: ghi đè mọi dòng (overwrite với điều kiện TRUE) —
    một commit bình thường, không phải tạo lại bảng, nên lịch sử snapshot vẫn còn.
    Nếu sau này đổi cột của bảng: DROP TABLE bảng đó trong Trino rồi chạy lại job.
    """
    if spark.catalog.tableExists(table):
        df.writeTo(table).overwrite(F.lit(True))
        return
    w = (df.writeTo(table).using("iceberg")
         .tableProperty("write.parquet.compression-codec", "zstd"))
    if partition_by_month:
        w = w.partitionedBy(F.months("event_time"))
    w.create()


def main():
    started = time.time()
    run_id = str(uuid.uuid4())
    spark = SparkSession.builder.appName("gold_scoring").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NS}")

    # 1. Đặc trưng ---------------------------------------------------------------
    log(f"Doc {SILVER_TABLE}, tinh dac trung theo the...")
    binned = (add_bins(with_split(card_features(spark.table(SILVER_TABLE)), SPLIT_TIME))
              .persist(StorageLevel.DISK_ONLY))
    counts = {r["split"]: (r["n"], r["frauds"]) for r in
              binned.groupBy("split").agg(F.count(F.lit(1)).alias("n"),
                                          F.sum("is_fraud").alias("frauds")).collect()}
    n_train, f_train = counts.get("train", (0, 0))
    n_test, f_test = counts.get("test", (0, 0))
    log(f"{n_train + n_test:,} giao dich | TRAIN (truoc {SPLIT_TIME}): {n_train:,} "
        f"({f_train or 0:,} gian lan) | TEST: {n_test:,} ({f_test or 0:,} gian lan)")
    if not n_train or not f_train:
        raise SystemExit("[gold] TRAIN khong co giao dich gian lan nao -> chua hoc duoc. "
                         "Da chay backfill (Buoc 2.7) chua?")

    # 2. Scorecard (chỉ học trên TRAIN) ------------------------------------------
    scorecard_rows = train_scorecard(binned.where("split = 'train'")).collect()
    log(f"Scorecard: {len(FEATURES)} dac trung, {len(scorecard_rows)} o. "
        "Suc manh (IV) tung dac trung:")
    for name, iv in information_values(scorecard_rows):
        level = ("manh" if iv > 0.3 else "trung binh" if iv >= 0.1
                 else "yeu" if iv >= 0.02 else "vo dung")
        log(f"   {name:<16} IV = {iv:7.3f}  ({level})")

    scored = apply_scorecard(binned, scorecard_rows).persist(StorageLevel.DISK_ONLY)

    # 3. Chọn ngưỡng trên TRAIN, đo trên TEST ------------------------------------
    thresholds = thresholds_for_rates(scored.where("split = 'train'"), ALERT_RATES)
    metrics = evaluate(scored, thresholds)
    aucs = roc_auc(scored)
    chosen = choose_threshold(metrics)
    threshold, chosen_rate = chosen["threshold"], chosen["target_alert_rate"]
    log(f"Nguong chon tren TRAIN (F1 cao nhat): score >= {threshold:.3f} "
        f"(canh bao ~{chosen['alert_rate']:.2%} giao dich train)")

    log("Bang danh doi tren TEST (du lieu scorecard CHUA TUNG thay):")
    log(f"   {'canh bao':>9} {'precision':>10} {'recall':>8} {'F1':>6}  nguong")
    for m in metrics:
        if m["split"] == "test":
            mark = "  <- nguong chon" if m["target_alert_rate"] == chosen_rate else ""
            log(f"   {m['alert_rate']:>9.2%} {m['precision']:>10.3f} {m['recall']:>8.3f} "
                f"{m['f1']:>6.3f}  {m['threshold']:.3f}{mark}")
    test_chosen = next((m for m in metrics
                        if m["split"] == "test" and m["target_alert_rate"] == chosen_rate), None)

    # 4. Ghi bảng Gold -----------------------------------------------------------
    final = scored.withColumn("is_alert", F.col("score") >= F.lit(threshold))
    keep = (["transaction_id", "card_token", "card_last4", "merchant", "category", "amount",
             "age", "distance_km", "event_time", "is_fraud", "ingest_source", "split"]
            + FEATURE_COLS + ["score", "is_alert", "reasons"])
    replace_table(spark, final.select(*keep), FEATURES_TABLE)
    replace_table(spark, final.where("is_alert").select(
        "transaction_id", "card_token", "card_last4", "event_time", "merchant", "category",
        "amount", "score", "reasons", "is_fraud", "split"), ALERTS_TABLE)
    replace_table(spark, spark.createDataFrame(scorecard_rows), SCORECARD_TABLE,
                  partition_by_month=False)

    spark.sql(CREATE_METRICS)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(run_id, now, m["split"], m["target_alert_rate"], m["threshold"], m["n"],
             m["alerts"], m["alert_rate"], m["tp"], m["fp"], m["fn"], m["precision"],
             m["recall"], m["f1"], aucs.get(m["split"]), m["target_alert_rate"] == chosen_rate)
            for m in metrics]
    spark.createDataFrame(rows, METRICS_SCHEMA).writeTo(METRICS_TABLE).append()

    # 5. Tóm tắt -----------------------------------------------------------------
    log("=" * 64)
    if test_chosen:
        auc = aucs.get("test")
        log(f"KET QUA TREN TEST ({n_test:,} giao dich, {test_chosen['tp'] + test_chosen['fn']:,} "
            "gian lan that):")
        log(f"   canh bao {test_chosen['alerts']:,} giao dich ({test_chosen['alert_rate']:.2%})")
        log(f"   precision {test_chosen['precision']:.3f}  = {test_chosen['tp']:,} canh bao dung"
            f" / {test_chosen['alerts']:,} canh bao")
        log(f"   recall    {test_chosen['recall']:.3f}  = bat duoc {test_chosen['tp']:,}"
            f" / {test_chosen['tp'] + test_chosen['fn']:,} vu gian lan")
        log(f"   F1 {test_chosen['f1']:.3f} | ROC-AUC {auc:.3f}" if auc is not None
            else f"   F1 {test_chosen['f1']:.3f}")
    else:
        log("Chua co du lieu TEST (producer chua phat fraudTest?) -> chi co ket qua TRAIN.")
    log(f"Da ghi {FEATURES_TABLE}, {ALERTS_TABLE}, {SCORECARD_TABLE}, {METRICS_TABLE} "
        f"(run_id {run_id[:8]}). Xong sau {time.time() - started:,.0f} giay.")

    scored.unpersist()
    binned.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
