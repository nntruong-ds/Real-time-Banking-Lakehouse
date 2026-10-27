-- ============================================================================
-- Kiểm tra DLQ, bỏ trùng, bảo mật PII và dữ liệu lịch sử.
-- Vào Trino:   docker exec -it trino trino      Thoát: quit
-- ============================================================================

-- ---------------------------------------------------------------- DLQ ------
-- 1. Bronze không lẫn bản ghi lỗi: KHÔNG được có dòng 'invalid'
SELECT sim_scenario, count(*) AS so_luong
FROM iceberg.bronze.transactions_raw
GROUP BY sim_scenario;

-- 2. Bản ghi lỗi trong DLQ, chia theo lý do
SELECT error_reason, count(*) AS so_luong
FROM iceberg.bronze.dlq
GROUP BY error_reason ORDER BY so_luong DESC;

-- 3. Đối chiếu: lỗi producer cố tình tạo  <->  luật đã bắt được.
--    Mỗi loi_co_y chỉ được khớp với đúng 1 luat_bat_duoc.
SELECT json_extract_scalar(raw_value, '$.sim_invalid_reason') AS loi_co_y,
       error_reason AS luat_bat_duoc,
       count(*) AS so_luong
FROM iceberg.bronze.dlq
GROUP BY 1, 2 ORDER BY 1;

-- ---------------------------------------------------------- BỎ TRÙNG ------
-- 4. Bronze CÓ bản trùng (producer cố ý gửi trùng), Silver phải = 0
SELECT 'bronze' AS tang, count(*) AS tong,
       count(*) - count(DISTINCT transaction_id) AS so_ban_trung
FROM iceberg.bronze.transactions_raw
UNION ALL
SELECT 'silver', count(*), count(*) - count(DISTINCT transaction_id)
FROM iceberg.silver.transactions;

-- --------------------------------------------------------------- PII ------
-- 5. Silver chỉ còn dữ liệu đã che: token, 4 số cuối, tuổi, khoảng cách
SELECT transaction_id, card_token, card_last4, gender, age, state,
       merchant, category, amount, distance_km, event_time, is_fraud
FROM iceberg.silver.transactions
ORDER BY silver_time DESC
LIMIT 5;

-- 6. Các cột của Silver: không được thấy card_number, first_name, street, dob...
SHOW COLUMNS FROM iceberg.silver.transactions;

-- 7. Cùng một thẻ luôn ra cùng một token: hai con số phải (gần) bằng nhau
SELECT (SELECT count(DISTINCT card_number) FROM iceberg.bronze.transactions_raw) AS so_the_bronze,
       (SELECT count(DISTINCT card_token)  FROM iceberg.silver.transactions)     AS so_token_silver;

-- ---------------------------------------------------- LỊCH SỬ + PHÂN TÍCH ----
-- 8. Silver theo tháng và nguồn nạp: 2019-01 -> 2020-06 là batch, sau đó là stream
SELECT date_format(event_time, '%Y-%m') AS thang, ingest_source,
       count(*) AS giao_dich, sum(is_fraud) AS gian_lan
FROM iceberg.silver.transactions
GROUP BY 1, 2 ORDER BY 1, 2;

-- 9. Gian lận theo danh mục: danh mục nào rủi ro nhất?
SELECT category, count(*) AS giao_dich,
       round(100.0 * sum(is_fraud) / count(*), 2) AS ty_le_gian_lan_pct,
       round(avg(CASE WHEN is_fraud = 1 THEN amount END), 2) AS tien_tb_khi_gian_lan
FROM iceberg.silver.transactions
GROUP BY category ORDER BY ty_le_gian_lan_pct DESC;

-- 10. Độ trễ đầu-cuối: từ lúc producer gửi tin vào Kafka tới lúc nằm trong Silver (giây),
--     tính trên các giao dịch của 10 phút gần nhất
SELECT round(approx_percentile(to_unixtime(s.silver_time) - to_unixtime(b.kafka_timestamp), 0.5), 1) AS p50_giay,
       round(approx_percentile(to_unixtime(s.silver_time) - to_unixtime(b.kafka_timestamp), 0.95), 1) AS p95_giay
FROM iceberg.silver.transactions s
JOIN iceberg.bronze.transactions_raw b ON s.transaction_id = b.transaction_id
WHERE b.ingest_source = 'stream'
  AND s.silver_time > current_timestamp - INTERVAL '10' MINUTE;
