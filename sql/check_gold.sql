-- ============================================================================
-- Tầng Gold: đặc trưng theo thẻ, scorecard, cảnh báo gian lận, kết quả đánh giá.
-- Chạy sau:    docker compose --profile jobs run --rm gold
-- Vào Trino:   docker exec -it trino trino      Thoát: quit
-- ============================================================================

-- 1. Các bảng Gold
SHOW TABLES FROM iceberg.gold;

-- 2. KẾT QUẢ của lần chạy mới nhất, ở ngưỡng đã chọn (con số để ghi CV là dòng 'test')
SELECT split, n AS giao_dich, alerts AS canh_bao,
       round(100 * alert_rate, 2) AS ty_le_canh_bao_pct,
       round(precision, 3) AS precision, round(recall, 3) AS recall,
       round(f1, 3) AS f1, round(roc_auc, 3) AS roc_auc, round(threshold, 3) AS nguong
FROM iceberg.gold.model_metrics
WHERE is_chosen
  AND run_time = (SELECT max(run_time) FROM iceberg.gold.model_metrics)
ORDER BY split DESC;

-- 3. Đánh đổi precision <-> recall trên TEST: cảnh báo nhiều hơn thì bắt được nhiều hơn,
--    nhưng tỷ lệ cảnh báo đúng giảm. Ngân hàng chọn theo sức người duyệt cảnh báo.
SELECT round(100 * alert_rate, 2) AS ty_le_canh_bao_pct, alerts AS canh_bao,
       round(precision, 3) AS precision, round(recall, 3) AS recall, round(f1, 3) AS f1,
       is_chosen AS nguong_chon
FROM iceberg.gold.model_metrics
WHERE split = 'test'
  AND run_time = (SELECT max(run_time) FROM iceberg.gold.model_metrics)
ORDER BY alert_rate;

-- 4. Đặc trưng nào mạnh nhất? (IV > 0.3 mạnh · 0.1-0.3 trung bình · < 0.02 vô dụng)
SELECT feature AS dac_trung, max(iv) AS iv, count(*) AS so_o
FROM iceberg.gold.scorecard
GROUP BY feature ORDER BY iv DESC;

-- 5. 15 "ô" đáng ngờ nhất: rơi vào ô này thì điểm rủi ro tăng mạnh nhất
SELECT feature AS dac_trung, bin AS o, n_total AS giao_dich, n_fraud AS gian_lan,
       round(100 * fraud_rate, 2) AS ty_le_gian_lan_pct, woe
FROM iceberg.gold.scorecard
WHERE n_total >= 100
ORDER BY woe DESC
LIMIT 15;

-- 6. Hàng đợi cảnh báo trên dữ liệu TEST: 10 giao dịch điểm cao nhất, kèm lý do
SELECT event_time, card_last4, category, amount, round(score, 2) AS diem,
       reasons AS ly_do, is_fraud AS gian_lan_that
FROM iceberg.gold.fraud_alerts
WHERE split = 'test'
ORDER BY score DESC
LIMIT 10;

-- 7. Gian lận bị BỎ SÓT trên TEST (để hiểu điểm yếu của scorecard)
SELECT category, count(*) AS bo_sot, round(avg(amount), 2) AS tien_tb,
       round(avg(score), 2) AS diem_tb
FROM iceberg.gold.transaction_features
WHERE split = 'test' AND is_fraud = 1 AND NOT is_alert
GROUP BY category ORDER BY bo_sot DESC;

-- 8. Đặc trưng theo thẻ: giao dịch gian lận khác giao dịch thường ra sao?
SELECT is_fraud AS gian_lan,
       round(avg(txn_count_24h), 2) AS tb_so_lan_quet_24h,
       round(approx_percentile(amount_ratio_30d, 0.5), 2) AS trung_vi_gap_may_lan_tb_30d,
       round(avg(CASE WHEN hour_of_day >= 22 OR hour_of_day < 4 THEN 1.0 ELSE 0 END), 3)
           AS ty_le_dem_khuya
FROM iceberg.gold.transaction_features
GROUP BY is_fraud;

-- 9. Lịch sử các lần chạy (mỗi lần chạy job gold thêm một bộ dòng)
SELECT run_id, run_time, round(precision, 3) AS precision, round(recall, 3) AS recall,
       round(roc_auc, 3) AS roc_auc
FROM iceberg.gold.model_metrics
WHERE split = 'test' AND is_chosen
ORDER BY run_time DESC;
