"""Kiểm tra các hàm bảo mật PII. Chạy:  pytest -q"""

import hashlib
import hmac
import math
from datetime import date, datetime

from pyspark.sql import functions as F

from common.pii import age_years, haversine_km, hmac_sha256, last4

CARDS = ["2703186189652095", "630423337322", "4613314721966", ""]


def python_hmac(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def test_hmac_matches_python_standard_library(spark, key):
    df = spark.createDataFrame([(c,) for c in CARDS], "card STRING")
    got = {r.card: r.token for r in
           df.select("card", hmac_sha256(F.col("card"), key).alias("token")).collect()}
    for card in CARDS:
        assert got[card] == python_hmac(card, key)


def test_hmac_long_key_is_hashed_first(spark):
    long_key = b"k" * 100
    df = spark.createDataFrame([("2703186189652095",)], "card STRING")
    assert df.select(hmac_sha256(F.col("card"), long_key)).first()[0] == \
        python_hmac("2703186189652095", long_key)


def test_hmac_differs_from_plain_sha256_and_depends_on_key(spark, key):
    df = spark.createDataFrame([("2703186189652095",)], "card STRING")
    t1 = df.select(hmac_sha256(F.col("card"), key)).first()[0]
    t2 = df.select(hmac_sha256(F.col("card"), b"another-key-" * 4)).first()[0]
    assert t1 != hashlib.sha256(b"2703186189652095").hexdigest()
    assert t1 != t2


def test_hmac_null_stays_null(spark, key):
    df = spark.createDataFrame([(None,)], "card STRING")
    assert df.select(hmac_sha256(F.col("card"), key)).first()[0] is None


def test_last4_and_age(spark):
    df = spark.createDataFrame(
        [("2703186189652095", date(1988, 3, 9), datetime(2019, 1, 1, 0, 0, 18)),
         ("2703186189652095", date(1988, 3, 9), datetime(2019, 3, 9, 12, 0, 0))],
        "card STRING, dob DATE, t TIMESTAMP")
    rows = df.select(last4(F.col("card")).alias("l4"),
                     age_years(F.col("dob"), F.col("t")).alias("age")).collect()
    assert rows[0].l4 == "2095"
    assert rows[0].age == 30   # chưa tới sinh nhật
    assert rows[1].age == 31   # đúng ngày sinh nhật


def python_haversine(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def test_haversine_matches_python(spark):
    pts = [(36.0788, -81.1781, 36.011293, -82.048315), (40.7128, -74.0060, 34.0522, -118.2437)]
    df = spark.createDataFrame(pts, "a DOUBLE, b DOUBLE, c DOUBLE, d DOUBLE")
    got = [r[0] for r in df.select(haversine_km(*[F.col(c) for c in "abcd"])).collect()]
    for p, g in zip(pts, got):
        assert abs(g - python_haversine(*p)) < 1e-6
    assert 3900 < got[1] < 4000    # New York - Los Angeles khoảng 3.940 km
