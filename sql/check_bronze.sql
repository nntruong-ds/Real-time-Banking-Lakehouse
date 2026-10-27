-- ============================================================================
-- Kiểm tra tầng Bronze bằng Trino.
-- Vào Trino:   docker exec -it trino trino      Thoát: quit
-- ============================================================================

-- 1. Các schema và bảng
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;

-- 2. Số bản ghi theo nguồn nạp (stream = Kafka, batch = backfill CSV)
SELECT ingest_source, count(*) AS so_ban_ghi,
       min(event_time) AS tu, max(event_time) AS den
FROM iceberg.bronze.transactions_raw
GROUP BY ingest_source;

-- 3. 10 giao dịch mới nhận (Bronze còn nguyên PII: chỉ người có quyền mới được xem)
SELECT transaction_id, card_number, first_name, last_name, merchant, amount, event_time
FROM iceberg.bronze.transactions_raw
ORDER BY ingest_time DESC
LIMIT 10;

-- 4. Tỷ lệ gian lận theo nhãn của dataset (khoảng 0,4–0,6%)
SELECT count(*) AS tong, sum(is_fraud) AS gian_lan,
       round(100.0 * sum(is_fraud) / count(*), 3) AS phan_tram
FROM iceberg.bronze.transactions_raw;

-- 5. Mỗi partition Kafka nhận bao nhiêu tin (phải gần đều nhau)
SELECT kafka_partition, count(*) AS so_luong
FROM iceberg.bronze.transactions_raw
WHERE ingest_source = 'stream'
GROUP BY kafka_partition ORDER BY kafka_partition;

-- 6. Không ghi trùng tin Kafka nào (exactly-once): hai số phải bằng nhau
SELECT count(*) AS tong,
       count(DISTINCT (kafka_partition, kafka_offset)) AS khong_trung
FROM iceberg.bronze.transactions_raw
WHERE ingest_source = 'stream';

-- 7. Số file và snapshot (dùng lại ở Giai đoạn 4 khi đo trước/sau compaction)
SELECT count(*) AS so_file, round(sum(file_size_in_bytes) / 1e6, 1) AS dung_luong_mb
FROM iceberg.bronze."transactions_raw$files";

SELECT count(*) AS so_snapshot
FROM iceberg.bronze."transactions_raw$snapshots";
