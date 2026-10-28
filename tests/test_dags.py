"""
Kiểm tra các DAG Airflow nạp được, đúng thứ tự task, đúng 18 tháng backfill.

Chỉ chạy khi máy có cài Airflow (CI cài riêng ở job "airflow"); không có thì tự bỏ qua.
"""

from pathlib import Path

import pytest

# Không dùng "airflow": thư mục airflow/ của dự án trùng tên gói nên import đó luôn "thành công"
pytest.importorskip("airflow.providers.docker")

DAGS = Path(__file__).resolve().parent.parent / "airflow" / "dags"


def _is_v2() -> bool:
    import airflow
    return airflow.__version__.startswith("2.")


def test_dags_import_and_structure(monkeypatch):
    monkeypatch.setenv("HOST_PROJECT_DIR", "/home/user/project")
    try:                                        # Airflow 3
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:                         # Airflow 2
        from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False) if _is_v2() else \
        DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    assert set(bag.dags) == {"lakehouse_daily", "lakehouse_backfill", "lakehouse_maintenance"}

    daily = bag.dags["lakehouse_daily"]
    assert daily.get_task("dbt_build").upstream_task_ids == {"gold_scoring"}
    gold = daily.get_task("gold_scoring")
    assert gold.network_mode == "lakehouse" and gold.mount_tmp_dir is False
    assert {m["Source"] for m in gold.mounts} == {
        "/home/user/project/spark_jobs", "/home/user/project/docker/spark/spark-defaults.conf"}

    backfill = bag.dags["lakehouse_backfill"].get_task("backfill_month")
    commands = backfill.expand_input.value["command"]
    assert len(commands) == 18
    assert commands[0].endswith("--month 2019-01") and commands[-1].endswith("--month 2020-06")
