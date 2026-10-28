# Airflow 3 + provider Docker (để DAG chạy các job Spark/dbt bằng container riêng).
FROM apache/airflow:3.3.2

# Ghim cả phiên bản Airflow để pip không lỡ tay nâng/hạ Airflow khi cài provider
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-docker==4.5.9"
# (4.5.9 = đúng bản mà Airflow 3.3.2 đã kiểm thử, theo file constraints chính thức)
