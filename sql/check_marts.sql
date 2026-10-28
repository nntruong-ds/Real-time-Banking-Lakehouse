-- ============================================================================
-- Giai đoạn 4: bảng báo cáo do dbt dựng (iceberg.marts) và nhật ký bảo trì Iceberg.
-- Chạy sau:    docker compose --profile jobs run --rm dbt
--              docker compose --profile jobs run --rm maintenance
-- Vào Trino:   docker exec -it trino trino      Thoát: quit
-- ============================================================================

-- 1. Các bảng dbt đã dựng
SHOW TABLES FROM iceberg.marts;

-- 2. SỐ TIỀN gian lận chặn được trên TEST (con số nghiệp vụ, đáng ghi vào CV)
SELECT sum(frauds) AS vu_gian_lan, sum(true_alerts) AS vu_bat_duoc,
       round(sum(fraud_amount), 0) AS tien_gian_lan_usd,
       round(sum(fraud_amount_caught), 0) AS tien_chan_duoc_usd,
       round(100.0 * sum(fraud_amount_caught) / sum(fraud_amount), 1) AS ty_le_tien_chan_duoc_pct
FROM iceberg.marts.fct_daily_fraud
WHERE split = 'test';

-- 3. 14 ngày cuối của TEST: mỗi ngày bao nhiêu gian lận, bao nhiêu cảnh báo
SELECT event_date, transactions, frauds, alerts, true_alerts, fraud_amount, fraud_amount_caught
FROM iceberg.marts.fct_daily_fraud
WHERE split = 'test'
ORDER BY event_date DESC
LIMIT 14;

-- 4. Thẻ theo mức rủi ro
SELECT risk_status, count(*) AS so_the, sum(frauds) AS gian_lan, sum(alerts) AS canh_bao
FROM iceberg.marts.dim_card_risk
GROUP BY risk_status ORDER BY so_the DESC;

-- 5. Danh mục có tỷ lệ gian lận cao nhất theo từng tháng của TEST
SELECT month_start, category, transactions, frauds, fraud_rate_pct
FROM iceberg.marts.agg_category_month
WHERE month_start >= DATE '2020-07-01'
ORDER BY month_start, fraud_rate_pct DESC;

-- 6. BẢO TRÌ ICEBERG: số file và thời gian truy vấn trước/sau lần gộp file gần nhất
SELECT table_name AS bang, files_before AS file_truoc, files_after AS file_sau,
       round(100.0 * (files_before - files_after) / files_before, 1) AS giam_file_pct,
       query_sec_before AS truy_van_truoc_s, query_sec_after AS truy_van_sau_s,
       round(query_sec_before / nullif(query_sec_after, 0), 1) AS nhanh_gap
FROM iceberg.ops.maintenance_log
WHERE run_time = (SELECT max(run_time) FROM iceberg.ops.maintenance_log)
ORDER BY files_before DESC;

-- 7. Đo lại bằng Trino: chạy câu này TRƯỚC và SAU khi bảo trì, so dòng "elapsed" Trino in ra
SELECT count(DISTINCT transaction_id) FROM iceberg.bronze.transactions_raw;
