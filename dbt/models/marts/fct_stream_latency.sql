-- Độ trễ đầu-cuối của luồng streaming theo từng GIỜ CHẠY THẬT (giờ máy, không phải giờ giao dịch):
-- p50 = một nửa số giao dịch tới Silver nhanh hơn mức này; p95 = 95% nhanh hơn mức này.
select
    date_trunc('hour', silver_time)                       as hour_start,
    count(*)                                              as transactions,
    round(approx_percentile(latency_sec, 0.5), 1)         as p50_sec,
    round(approx_percentile(latency_sec, 0.95), 1)        as p95_sec,
    round(max(latency_sec), 1)                            as max_sec
from {{ ref('int_stream_latency') }}
group by 1
