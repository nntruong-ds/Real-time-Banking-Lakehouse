"""
Kiểm tra tầng Gold: đặc trưng không "nhìn trộm tương lai", công thức WoE, cách chấm điểm,
và các thước đo precision / recall / ROC-AUC.
"""

import math
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from common.gold import (FEATURE_NAMES, add_bins, apply_scorecard, bin_by_edges, bin_counts,
                         card_features, choose_threshold, evaluate, information_values, prf,
                         roc_auc, thresholds_for_rates, train_scorecard, with_split)
from common.tables import dedup_batch, to_silver, valid_rows
from helpers import validated_from_events
from replay import row_to_event
from sample_data import make_rows

T0 = datetime(2020, 1, 1, 8, 0, 0)
COLS = ["transaction_id", "card_token", "card_last4", "merchant", "category", "amount",
        "age", "distance_km", "event_time", "is_fraud", "ingest_source"]


def silver(spark, rows):
    """rows: (id, card, phút tính từ T0, số tiền, danh mục, is_fraud)."""
    data = [(tid, card, card[-4:], "m", cat, float(amt), 40, 10.0,
             T0 + timedelta(minutes=mins), fraud, "batch")
            for tid, card, mins, amt, cat, fraud in rows]
    return spark.createDataFrame(data, COLS)


# ------------------------------------------------------------------ đặc trưng -----
def test_card_features_only_look_back(spark):
    rows = [("a1", "card_A", 0, 10, "home", 0),
            ("a2", "card_A", 30, 20, "home", 0),
            ("a3", "card_A", 50, 30, "home", 0),
            ("a4", "card_A", 120, 40, "home", 0),
            ("a5", "card_A", 26 * 60, 50, "home", 0),
            ("b1", "card_B", 10, 999, "home", 1)]
    got = {r.transaction_id: r for r in card_features(silver(spark, rows)).collect()}

    first = got["a1"]
    assert (first.txn_count_1h, first.txn_count_24h, first.amount_sum_24h) == (0, 0, 0.0)
    assert first.secs_since_prev is None and first.amount_ratio_30d is None

    assert (got["a2"].txn_count_1h, got["a2"].secs_since_prev, got["a2"].amount_ratio_30d) \
        == (1, 1800, 2.0)
    assert (got["a3"].txn_count_1h, got["a3"].txn_count_24h, got["a3"].amount_sum_24h) \
        == (2, 2, 30.0)
    # a4: giao dịch gần nhất cách 70 phút -> 0 lần trong 1 giờ, 3 lần trong 24 giờ
    assert (got["a4"].txn_count_1h, got["a4"].txn_count_24h, got["a4"].card_avg_amount_30d) \
        == (0, 3, 20.0)
    # a5: đúng 24 giờ sau a4 -> chỉ a4 nằm trong cửa sổ 24 giờ
    assert (got["a5"].txn_count_24h, got["a5"].amount_sum_24h, got["a5"].amount_ratio_30d) \
        == (1, 40.0, 2.0)
    # thẻ khác không ảnh hưởng nhau
    assert (got["b1"].txn_count_24h, got["b1"].secs_since_prev) == (0, None)
    assert got["a1"].hour_of_day == 8


def test_bins(spark):
    df = spark.createDataFrame([(5.0, 0, None), (20.0, 3, 0.4), (999.99, 12, 12.0),
                                (1000.0, 1, None)], "amount double, n int, ratio double")
    got = df.select(
        bin_by_edges(F.col("amount"), [20, 50, 100, 200, 300, 500, 800, 1000]).alias("a"),
        bin_counts(F.col("n"), [0, 1, 2, 3, 5, 8, 12]).alias("c"),
        bin_by_edges(F.col("ratio"), [0.5, 1, 2, 5, 10], null_label="no_history").alias("r"),
    ).collect()
    assert [tuple(r) for r in got] == [("<20", "0", "no_history"),
                                       ("20-50", "3-4", "<0.5"),
                                       ("800-1000", "12+", ">=10"),
                                       (">=1000", "1", "no_history")]


# ------------------------------------------------------------------ scorecard -----
def _toy_binned(spark):
    rows = ([(f"f{i}", f"card_{i % 7}", i * 600, 900, "shopping_net", 1) for i in range(40)]
            + [(f"l{i}", f"card_{i % 7}", i * 600 + 7, 30, "grocery_pos", 0)
               for i in range(160)])
    return add_bins(card_features(silver(spark, rows)))


def test_scorecard_woe_formula(spark):
    sc = {(r.feature, r.bin): r for r in train_scorecard(_toy_binned(spark)).collect()}
    risky = sc[("category_amount", "shopping_net|800-1000")]
    safe = sc[("category_amount", "grocery_pos|20-50")]
    # 40/40 gian lận, 0/160 giao dịch thường: WoE = ln((40.5/40.5) / (0.5/160.5))
    assert risky.woe == pytest.approx(round(math.log(160.5 / 0.5), 4))
    assert safe.woe == pytest.approx(round(math.log((0.5 / 40.5) / (160.5 / 160.5)), 4))
    assert risky.fraud_rate == 1.0 and safe.fraud_rate == 0.0
    ivs = dict(information_values(list(sc.values())))
    assert set(ivs) == set(FEATURE_NAMES)
    assert ivs["category_amount"] > 0.3          # đặc trưng tách gian lận hoàn hảo -> IV mạnh


def test_apply_scorecard_sums_woe_and_explains(spark):
    binned = add_bins(card_features(silver(spark, [
        ("x", "card_X", 15 * 60, 900, "shopping_net", 1),     # 23:00, 900 USD
        ("y", "card_Y", 0, 15, "kids_pets", 0)])))            # 08:00, 15 USD
    scorecard = [
        {"feature": "hour", "bin": "23", "woe": 2.0},
        {"feature": "category_amount", "bin": "shopping_net|800-1000", "woe": 1.5},
        {"feature": "category_amount", "bin": "kids_pets|<20", "woe": -0.8},
        {"feature": "age_band", "bin": "35-45", "woe": 0.1},
        {"feature": "gap_since_prev", "bin": "first_txn", "woe": -0.2},
    ]
    got = {r.transaction_id: r for r in apply_scorecard(binned, scorecard).collect()}
    # ô không có trong scorecard (vd giờ 08) -> WoE 0
    assert got["x"].score == pytest.approx(2.0 + 1.5 + 0.1 - 0.2)
    assert got["y"].score == pytest.approx(-0.8 + 0.1 - 0.2)
    assert got["x"].reasons == ["hour=23", "category_amount=shopping_net|800-1000",
                                "age_band=35-45"]
    assert got["y"].reasons == ["age_band=35-45"]       # chỉ giữ lý do có WoE dương


# ------------------------------------------------------------------ đánh giá ------
def test_prf_and_choose_threshold():
    assert prf(8, 2, 8) == pytest.approx((0.8, 0.5, 2 * 0.8 * 0.5 / 1.3))
    assert prf(0, 0, 5) == (0.0, 0.0, 0.0)
    metrics = [{"split": "train", "target_alert_rate": 0.01, "threshold": 3.0, "f1": 0.4},
               {"split": "train", "target_alert_rate": 0.02, "threshold": 2.0, "f1": 0.6},
               {"split": "test", "target_alert_rate": 0.05, "threshold": 1.0, "f1": 0.9}]
    assert choose_threshold(metrics)["threshold"] == 2.0     # không bao giờ chọn bằng TEST


def test_evaluate_and_auc(spark):
    df = spark.createDataFrame(
        [("test", 3.0, 1), ("test", 2.0, 0), ("test", 1.0, 1), ("test", 0.0, 0),
         ("perfect", 9.0, 1), ("perfect", 1.0, 0), ("ties", 5.0, 1), ("ties", 5.0, 0)],
        "split string, score double, is_fraud int")
    m = {(x["split"], x["threshold"]): x for x in evaluate(df, [(0.5, 1.5)])}
    t = m[("test", 1.5)]
    assert (t["tp"], t["fp"], t["fn"], t["alerts"], t["n"]) == (1, 1, 1, 2, 4)
    assert (t["precision"], t["recall"]) == (0.5, 0.5)

    auc = roc_auc(df)
    assert auc["test"] == pytest.approx(0.75)    # 3 trong 4 cặp (gian lận, thường) xếp đúng
    assert auc["perfect"] == pytest.approx(1.0)
    assert auc["ties"] == pytest.approx(0.5)     # điểm bằng nhau = đoán mò


# ------------------------------------------------------------------ đầu-cuối ------
def test_end_to_end_on_sample_data(spark, key):
    events = [row_to_event(r) for r in make_rows(600, seed=11)]
    silver_df = to_silver(dedup_batch(valid_rows(validated_from_events(spark, events))), key)
    times = sorted(r.event_time for r in silver_df.select("event_time").collect())
    split_time = times[len(times) * 2 // 3].strftime("%Y-%m-%d %H:%M:%S")

    binned = add_bins(with_split(card_features(silver_df), split_time)).cache()
    scorecard = train_scorecard(binned.where("split = 'train'")).collect()
    scored = apply_scorecard(binned, scorecard).cache()
    thresholds = thresholds_for_rates(scored.where("split = 'train'"), [0.05, 0.1])
    metrics = evaluate(scored, thresholds)

    assert scored.count() == silver_df.count()
    assert {m["split"] for m in metrics} == {"train", "test"}
    assert choose_threshold(metrics)["split"] == "train"
    assert set(roc_auc(scored)) == {"train", "test"}
    assert scored.where("score IS NULL").count() == 0
