"""Kiểm tra các phép tính báo cáo của job bảo trì Iceberg (phần không cần Iceberg)."""

from iceberg_maintenance import median_seconds, pct_less, short_name, speedup


def test_short_name():
    assert short_name("lakehouse.bronze.transactions_raw") == "bronze.transactions_raw"
    assert short_name("lakehouse.gold.fraud_alerts") == "gold.fraud_alerts"


def test_report_math():
    assert pct_less(2000, 20) == 99.0
    assert pct_less(10, 12) == -20.0          # tăng thì ra số âm, không giấu đi
    assert pct_less(0, 0) == 0.0
    assert speedup(12.0, 3.0) == 4.0
    assert speedup(1.0, 0.0) == 0.0


def test_median_seconds_runs_n_times():
    calls = []
    assert median_seconds(lambda: calls.append(1), runs=3) >= 0
    assert len(calls) == 3
