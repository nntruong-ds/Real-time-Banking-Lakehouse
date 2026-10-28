"""
3 DAG điều phối lakehouse — MỘT file duy nhất để chép được vào thư mục dags của BẤT KỲ
Airflow nào (Airflow 2.4+ hoặc 3.x, Airflow riêng của dự án hay Airflow có sẵn trên máy).

    lakehouse_daily        hằng ngày 02:00 UTC : gold_scoring -> dbt_build
    lakehouse_backfill     chạy tay            : 18 task, mỗi task nạp một tháng fraudTrain
    lakehouse_maintenance  Chủ nhật 03:00 UTC  : compact_tables (gộp file nhỏ Iceberg)

Mỗi task chạy trong MỘT CONTAINER RIÊNG (DockerOperator), dùng đúng image mà docker compose
dùng: Airflow chỉ điều phối (khi nào chạy, thứ tự, chạy lại khi lỗi), việc nặng do container
Spark/dbt làm rồi tự xóa khi xong.

Airflow cần 3 thứ (xem README / sổ tay Bước 4.4):
  - quyền dùng Docker trên máy: gắn /var/run/docker.sock + nhóm docker (group_add)
  - biến HOST_PROJECT_DIR = đường dẫn TUYỆT ĐỐI tới thư mục dự án TRÊN MÁY
  - biến PII_HMAC_KEY (job backfill cần để băm số thẻ)
"""

import os
from datetime import timedelta

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

try:                                    # Airflow 3
    from airflow.sdk import DAG
except ImportError:                     # Airflow 2.4+
    from airflow import DAG

PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")
if not os.path.isabs(PROJECT_DIR):
    # Hiện thành lỗi nạp DAG trên giao diện Airflow, thay vì để container Spark lỗi khó hiểu
    raise RuntimeError(
        "HOST_PROJECT_DIR chua dat hoac khong phai duong dan tuyet doi (dang la '%s'). "
        "Dat bien nay cho Airflow = thu muc du an tren may, roi khoi dong lai Airflow."
        % PROJECT_DIR)

SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "lakehouse-spark:3.5.9-iceberg1.10.0")
DBT_IMAGE = os.environ.get("DBT_IMAGE", "lakehouse-dbt:1.11.16")
NETWORK = os.environ.get("LAKEHOUSE_NETWORK", "lakehouse")
DOCKER_URL = "unix://var/run/docker.sock"
MONTHS = [f"2019-{m:02d}" for m in range(1, 13)] + [f"2020-{m:02d}" for m in range(1, 7)]
START = pendulum.datetime(2026, 10, 1, tz="UTC")


# ------------------------------------------------------------------ dựng task -------
def _bind(path_in_project: str, target: str) -> Mount:
    return Mount(target=target, source=os.path.join(PROJECT_DIR, path_in_project),
                 type="bind", read_only=True)


def spark_submit(script: str, *args: str) -> str:
    """Lệnh spark-submit giống hệt service 'gold'/'backfill' trong docker-compose.yml."""
    name = script[:-3] if script.endswith(".py") else script
    return " ".join(["spark-submit", "--master", "local[2]", "--driver-memory", "2g",
                     "--name", name, f"/opt/app/spark_jobs/{script}", *args])


def spark_kwargs(with_datasets: bool = False) -> dict:
    """Tham số chung của một container Spark (dùng được cả cho .partial() khi map task)."""
    mounts = [_bind("spark_jobs", "/opt/app/spark_jobs"),
              _bind("docker/spark/spark-defaults.conf", "/opt/spark/conf/spark-defaults.conf")]
    if with_datasets:
        mounts.append(_bind("datasets", "/data"))
    return dict(
        image=SPARK_IMAGE,
        environment={"AWS_ACCESS_KEY_ID": "admin", "AWS_SECRET_ACCESS_KEY": "password",
                     "AWS_REGION": "us-east-1"},
        # không hiện trên giao diện Airflow
        private_environment={"PII_HMAC_KEY": os.environ.get("PII_HMAC_KEY", "")},
        mounts=mounts,
        network_mode=NETWORK,
        docker_url=DOCKER_URL,
        mount_tmp_dir=False,      # Airflow cũng chạy trong container: không gắn thư mục tạm
        auto_remove="force",      # xong (kể cả lỗi) thì xóa container; log đã nằm trong Airflow
        mem_limit="3g",
    )


def spark_job(task_id: str, script: str, *args: str) -> DockerOperator:
    return DockerOperator(task_id=task_id, command=spark_submit(script, *args),
                          **spark_kwargs())


def dbt_job(task_id: str, command: str = "build") -> DockerOperator:
    return DockerOperator(task_id=task_id, image=DBT_IMAGE, command=command,
                          mounts=[_bind("dbt", "/dbt")], network_mode=NETWORK,
                          docker_url=DOCKER_URL, mount_tmp_dir=False, auto_remove="force",
                          mem_limit="1g")


# ------------------------------------------------------------------ 3 DAG -----------
with DAG(
    dag_id="lakehouse_daily",
    description="Gold + diem gian lan, roi dbt build (mart + test doi soat)",
    schedule="0 2 * * *",                  # 09:00 giờ Việt Nam
    start_date=START,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["lakehouse", "gold", "dbt"],
):
    # Một test đối soát thất bại -> dbt_build đỏ -> biết dữ liệu có vấn đề trước khi dùng báo cáo
    spark_job("gold_scoring", "gold_scoring.py") >> dbt_job("dbt_build", "build")


with DAG(
    dag_id="lakehouse_backfill",
    description="Nap lich su fraudTrain theo thang (18 task, idempotent)",
    schedule=None,                         # chỉ chạy khi bấm Trigger
    start_date=START,
    catchup=False,
    max_active_tasks=1,                    # lần lượt từng tháng: mỗi lúc một container Spark
    tags=["lakehouse", "backfill"],
):
    # Tháng nào lỗi thì Clear đúng ô tháng đó; job dùng MERGE nên chạy lại ra "+0 dong"
    DockerOperator.partial(
        task_id="backfill_month", **spark_kwargs(with_datasets=True),
    ).expand(command=[spark_submit("batch_backfill.py", "--month", m) for m in MONTHS])


with DAG(
    dag_id="lakehouse_maintenance",
    description="Compaction Iceberg + do truoc/sau",
    schedule="0 3 * * 0",                  # Chủ nhật
    start_date=START,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg"],
):
    spark_job("compact_tables", "iceberg_maintenance.py")
