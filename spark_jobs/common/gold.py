"""
Tầng Gold — đặc trưng theo thẻ và chấm điểm gian lận bằng scorecard WoE.

    Silver ─► card_features() ─► add_bins() ─► train_scorecard()  (chỉ học trên phần TRAIN)
                                            └► apply_scorecard() ─► điểm + lý do cho MỌI giao dịch
                                                                 └► đo precision/recall trên TEST

1) ĐẶC TRƯNG THEO THẺ — chỉ nhìn về QUÁ KHỨ của từng giao dịch (không "nhìn trộm tương lai"):
   txn_count_1h / 24h    số lần thẻ đó đã quẹt trong 1 giờ / 24 giờ TRƯỚC giao dịch này
   amount_sum_24h        tổng tiền thẻ đã tiêu trong 24 giờ trước
   amount_ratio_30d      số tiền lần này gấp mấy lần mức trung bình của thẻ trong 30 ngày trước
   secs_since_prev       cách lần quẹt trước bao nhiêu giây
   hour_of_day, age, distance_km, category, amount   (có sẵn ở Silver)

2) SCORECARD WoE (Weight of Evidence) — cách chấm điểm rủi ro kinh điển của ngân hàng:
   - Chia mỗi đặc trưng thành các "ô" (bin), ví dụ số tiền 800-1000 USD, giờ 23...
   - Với mỗi ô, trên dữ liệu TRAIN:  WoE = ln( % giao dịch gian lận rơi vào ô
                                              / % giao dịch bình thường rơi vào ô )
     WoE > 0: ô này hay gặp ở gian lận hơn ở giao dịch thường (càng lớn càng đáng ngờ).
     WoE < 0: ô này "an toàn".
   - Điểm của một giao dịch = tổng WoE của các ô nó rơi vào. Điểm >= ngưỡng -> cảnh báo.
   - Lý do cảnh báo = 3 ô có WoE dương lớn nhất (giải thích được cho người duyệt).
   - IV (Information Value) của mỗi đặc trưng cho biết đặc trưng đó mạnh tới đâu:
     < 0.02 vô dụng, 0.1-0.3 trung bình, > 0.3 mạnh.

3) ĐÁNH GIÁ — học trên TRAIN (fraudTrain, tới 21/06/2020), đo trên TEST (fraudTest, 6 tháng
   cuối 2020) mà scorecard chưa từng thấy. Ngưỡng chọn trên TRAIN (F1 cao nhất), giữ nguyên
   khi đo TEST -> con số trên TEST là con số trung thực.

Mọi hàm ở đây là phép biến đổi DataFrame thuần (không đọc/ghi bảng) để test được
không cần Iceberg: tests/test_gold.py.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Tỷ lệ cảnh báo (trên tổng số giao dịch) dùng để dò ngưỡng: 0.25% ... 5%
ALERT_RATES = [0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05]

SILVER_INPUT_COLS = ["transaction_id", "card_token", "card_last4", "merchant", "category",
                     "amount", "age", "distance_km", "event_time", "is_fraud", "ingest_source"]

FEATURE_COLS = ["txn_count_1h", "txn_count_24h", "amount_sum_24h", "card_avg_amount_30d",
                "amount_ratio_30d", "secs_since_prev", "hour_of_day"]

DAY = 86400


# ----------------------------------------------------------------- 1. đặc trưng -----
def card_features(silver: DataFrame) -> DataFrame:
    """Thêm đặc trưng theo thẻ. Mỗi cửa sổ chỉ gồm các giao dịch TRƯỚC giao dịch hiện tại."""
    by_card = Window.partitionBy("card_token").orderBy("ts")
    last_1h = by_card.rangeBetween(-3600, -1)
    last_24h = by_card.rangeBetween(-DAY, -1)
    last_30d = by_card.rangeBetween(-30 * DAY, -1)
    in_order = Window.partitionBy("card_token").orderBy("ts", "transaction_id")

    return (silver.select(*SILVER_INPUT_COLS)
            .withColumn("ts", F.col("event_time").cast("long"))
            .withColumn("txn_count_1h", F.count(F.lit(1)).over(last_1h))
            .withColumn("txn_count_24h", F.count(F.lit(1)).over(last_24h))
            .withColumn("amount_sum_24h",
                        F.round(F.coalesce(F.sum("amount").over(last_24h), F.lit(0.0)), 2))
            .withColumn("card_avg_amount_30d", F.round(F.avg("amount").over(last_30d), 2))
            .withColumn("amount_ratio_30d",
                        F.round(F.col("amount") / F.avg("amount").over(last_30d), 3))
            .withColumn("secs_since_prev", F.col("ts") - F.lag("ts").over(in_order))
            .withColumn("hour_of_day", F.hour("event_time"))
            .drop("ts"))


def with_split(df: DataFrame, split_time: str) -> DataFrame:
    """train = trước split_time (dữ liệu lịch sử để học), test = từ split_time trở đi."""
    return df.withColumn(
        "split", F.when(F.col("event_time") < F.to_timestamp(F.lit(split_time)), "train")
                  .otherwise("test"))


# ----------------------------------------------------------------- 2. chia ô --------
def _num(x) -> str:
    return f"{x:g}"


def bin_by_edges(col: Column, edges: list, labels: list = None,
                 null_label: str = "missing") -> Column:
    """Chia số thành các khoảng [.., e1), [e1, e2), .., [e_n, ..).

    Nhãn mặc định: '<20', '20-50', ..., '>=1000'. Giá trị NULL -> null_label.
    """
    if labels is None:
        labels = ([f"<{_num(edges[0])}"]
                  + [f"{_num(a)}-{_num(b)}" for a, b in zip(edges, edges[1:])]
                  + [f">={_num(edges[-1])}"])
    assert len(labels) == len(edges) + 1
    expr = F.when(col.isNull(), F.lit(null_label))
    for edge, label in zip(edges, labels):
        expr = expr.when(col < edge, F.lit(label))
    return expr.otherwise(F.lit(labels[-1]))


def bin_counts(col: Column, starts: list) -> Column:
    """Đếm nguyên: starts=[0,1,2,3,5] -> '0', '1', '2', '3-4', '5+'."""
    labels = []
    for a, b in zip(starts, starts[1:]):
        labels.append(str(a) if b == a + 1 else f"{a}-{b - 1}")
    labels.append(f"{starts[-1]}+")
    return bin_by_edges(col, starts[1:], labels)


AMOUNT_EDGES = [20, 50, 100, 200, 300, 500, 800, 1000]

# (tên đặc trưng, ý nghĩa, cách chia ô). Tên đặc trưng xuất hiện trong lý do cảnh báo.
FEATURES = [
    ("category_amount", "danh muc ket hop khoang so tien (USD)",
     lambda: F.concat_ws("|", F.coalesce(F.col("category"), F.lit("missing")),
                         bin_by_edges(F.col("amount"), AMOUNT_EDGES))),
    ("hour", "gio trong ngay (UTC)",
     lambda: F.coalesce(F.lpad(F.col("hour_of_day").cast("string"), 2, "0"), F.lit("missing"))),
    ("age_band", "nhom tuoi chu the",
     lambda: bin_by_edges(F.col("age"), [25, 35, 45, 55, 65, 75])),
    ("txn_count_1h", "so lan quet the trong 1 gio truoc",
     lambda: bin_counts(F.col("txn_count_1h"), [0, 1, 2, 3])),
    ("txn_count_24h", "so lan quet the trong 24 gio truoc",
     lambda: bin_counts(F.col("txn_count_24h"), [0, 1, 2, 3, 5, 8, 12])),
    ("gap_since_prev", "khoang cach toi lan quet truoc",
     lambda: bin_by_edges(F.col("secs_since_prev"),
                          [60, 600, 3600, 6 * 3600, DAY, 7 * DAY],
                          ["<1m", "1-10m", "10-60m", "1-6h", "6-24h", "1-7d", ">=7d"],
                          null_label="first_txn")),
    ("amount_vs_30d", "so tien gap may lan muc trung binh 30 ngay cua the",
     lambda: bin_by_edges(F.col("amount_ratio_30d"), [0.5, 1, 2, 5, 10],
                          null_label="no_history")),
    ("distance_band", "khoang cach nha khach - cua hang (km)",
     lambda: bin_by_edges(F.col("distance_km"), [25, 50, 75, 100])),
]
FEATURE_NAMES = [name for name, _, _ in FEATURES]


def add_bins(df: DataFrame) -> DataFrame:
    for name, _, build in FEATURES:
        df = df.withColumn(f"bin_{name}", build())
    return df


# ----------------------------------------------------------------- 3. scorecard -----
def train_scorecard(binned_train: DataFrame) -> DataFrame:
    """Học WoE cho từng (đặc trưng, ô) trên dữ liệu TRAIN. Trả về bảng scorecard."""
    pairs = F.array(*[F.struct(F.lit(n).alias("feature"), F.col(f"bin_{n}").alias("bin"))
                      for n in FEATURE_NAMES])
    counts = (binned_train
              .select(F.col("is_fraud").cast("long").alias("is_fraud"),
                      F.explode(pairs).alias("p"))
              .groupBy(F.col("p.feature").alias("feature"), F.col("p.bin").alias("bin"))
              .agg(F.count(F.lit(1)).alias("n_total"), F.sum("is_fraud").alias("n_fraud")))

    per_feature = Window.partitionBy("feature")
    frauds = F.sum("n_fraud").over(per_feature)            # tổng số gian lận (train)
    legits = F.sum("n_total").over(per_feature) - frauds   # tổng số giao dịch thường
    n_legit = F.col("n_total") - F.col("n_fraud")
    # +0.5 để ô không có gian lận nào vẫn có WoE hữu hạn (làm trơn Laplace)
    woe = F.log(((F.col("n_fraud") + 0.5) / (frauds + 0.5)) / ((n_legit + 0.5) / (legits + 0.5)))
    iv_part = (F.col("n_fraud") / frauds - n_legit / legits) * woe

    return (counts
            .withColumn("woe", woe)
            .withColumn("iv", F.sum(iv_part).over(per_feature))
            .select("feature", "bin", "n_total", "n_fraud",
                    F.round(F.col("n_fraud") / F.col("n_total"), 5).alias("fraud_rate"),
                    F.round("woe", 4).alias("woe"),
                    F.round("iv", 4).alias("iv"))
            .orderBy("feature", F.desc("woe")))


def apply_scorecard(binned: DataFrame, scorecard_rows: list) -> DataFrame:
    """Gắn score (tổng WoE) và reasons (tối đa 3 ô đáng ngờ nhất) cho mọi giao dịch.

    Scorecard nhỏ (vài trăm dòng) -> đưa vào biểu thức map, không cần join hay shuffle.
    Ô chưa từng gặp khi học -> WoE = 0 (không cộng, không trừ).
    """
    by_feature = {}
    for r in scorecard_rows:
        by_feature.setdefault(r["feature"], {})[r["bin"]] = float(r["woe"])

    parts = []
    for name in FEATURE_NAMES:
        bin_col = F.col(f"bin_{name}")
        table = by_feature.get(name, {})
        if table:
            lookup = F.create_map(*[x for b in sorted(table)
                                    for x in (F.lit(b), F.lit(table[b]))])
            woe = F.coalesce(F.try_element_at(lookup, bin_col), F.lit(0.0))
        else:
            woe = F.lit(0.0)
        parts.append(F.struct(woe.alias("woe"),
                              F.concat(F.lit(name + "="), bin_col).alias("reason")))
    contrib = F.array(*parts)
    top3 = F.slice(F.sort_array(contrib, asc=False), 1, 3)
    return (binned
            .withColumn("score", F.round(F.aggregate(contrib, F.lit(0.0),
                                                     lambda acc, x: acc + x["woe"]), 4))
            .withColumn("reasons", F.transform(F.filter(top3, lambda x: x["woe"] > 0),
                                               lambda x: x["reason"])))


def information_values(scorecard_rows: list) -> list:
    """[(feature, iv)] xếp từ mạnh tới yếu."""
    iv = {r["feature"]: float(r["iv"]) for r in scorecard_rows}
    return sorted(iv.items(), key=lambda kv: -kv[1])


# ----------------------------------------------------------------- 4. đánh giá ------
def prf(tp: int, fp: int, fn: int) -> tuple:
    """precision, recall, F1 (0 khi mẫu số = 0)."""
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return precision, recall, f1


def thresholds_for_rates(scored_train: DataFrame, rates: list = None) -> list:
    """Ngưỡng điểm sao cho khoảng `rate` giao dịch TRAIN bị cảnh báo. [(rate, threshold)]."""
    rates = rates or ALERT_RATES
    qs = scored_train.approxQuantile("score", [1 - r for r in rates], 1e-4)
    return list(zip(rates, qs))


def evaluate(scored: DataFrame, thresholds: list) -> list:
    """Ma trận nhầm lẫn + precision/recall/F1 cho mỗi (split, ngưỡng) trong MỘT lần quét."""
    fraud = F.col("is_fraud") == 1
    aggs = []
    for i, (_, thr) in enumerate(thresholds):
        alert = F.col("score") >= F.lit(thr)
        aggs += [F.sum(F.when(alert & fraud, 1).otherwise(0)).alias(f"tp{i}"),
                 F.sum(F.when(alert & ~fraud, 1).otherwise(0)).alias(f"fp{i}"),
                 F.sum(F.when(~alert & fraud, 1).otherwise(0)).alias(f"fn{i}")]
    rows = scored.groupBy("split").agg(F.count(F.lit(1)).alias("n"), *aggs).collect()

    out = []
    for row in rows:
        for i, (rate, thr) in enumerate(thresholds):
            tp, fp, fn = int(row[f"tp{i}"]), int(row[f"fp{i}"]), int(row[f"fn{i}"])
            precision, recall, f1 = prf(tp, fp, fn)
            out.append({"split": row["split"], "target_alert_rate": float(rate),
                        "threshold": float(thr), "n": int(row["n"]), "alerts": tp + fp,
                        "alert_rate": (tp + fp) / row["n"] if row["n"] else 0.0,
                        "tp": tp, "fp": fp, "fn": fn,
                        "precision": precision, "recall": recall, "f1": f1})
    return sorted(out, key=lambda m: (m["split"], m["target_alert_rate"]))


def choose_threshold(metrics: list) -> dict:
    """Ngưỡng có F1 cao nhất trên TRAIN (không bao giờ chọn bằng TEST)."""
    train = [m for m in metrics if m["split"] == "train"]
    if not train:
        raise ValueError("Khong co du lieu train de chon nguong")
    return max(train, key=lambda m: (m["f1"], -m["target_alert_rate"]))


def roc_auc(scored: DataFrame) -> dict:
    """ROC-AUC theo từng split, tính chính xác bằng thứ hạng (Mann-Whitney), xử lý điểm bằng nhau.

    AUC = xác suất một giao dịch gian lận ngẫu nhiên có điểm CAO HƠN một giao dịch thường
    ngẫu nhiên. 0.5 = đoán mò, 1.0 = tách hoàn hảo.
    """
    g = (scored.groupBy("split", "score")
         .agg(F.count(F.lit(1)).alias("n"), F.sum(F.col("is_fraud").cast("long")).alias("pos")))
    upto = (Window.partitionBy("split").orderBy("score")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    g = g.withColumn("avg_rank", F.sum("n").over(upto) - (F.col("n") - 1) / 2.0)
    rows = (g.groupBy("split")
            .agg(F.sum("pos").alias("P"), (F.sum("n") - F.sum("pos")).alias("N"),
                 F.sum(F.col("pos") * F.col("avg_rank")).alias("S"))
            .collect())
    out = {}
    for r in rows:
        p, n, s = int(r["P"] or 0), int(r["N"] or 0), float(r["S"] or 0.0)
        out[r["split"]] = (s - p * (p + 1) / 2) / (p * n) if p and n else None
    return out
