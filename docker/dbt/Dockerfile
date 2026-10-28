# Image chạy dbt (kết nối Trino). Ghim phiên bản để lần chạy nào cũng giống nhau.
FROM python:3.12.7-slim

RUN pip install --no-cache-dir dbt-core==1.11.16 dbt-trino==1.10.6

# Không ghi file tạm vào thư mục dự án (gắn read-only), không gửi thống kê ra ngoài
ENV DBT_PROFILES_DIR=/dbt \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DO_NOT_TRACK=1

WORKDIR /dbt
ENTRYPOINT ["dbt"]
CMD ["build"]
