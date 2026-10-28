-- Danh mục x tháng: quy mô chi tiêu và tỷ lệ gian lận theo thời gian (cho dashboard Giai đoạn 5).
select
    cast(date_trunc('month', event_time) as date)    as month_start,
    category,
    count(*)                                         as transactions,
    round(sum(amount), 2)                            as amount_total,
    sum(is_fraud)                                    as frauds,
    round(100.0 * sum(is_fraud) / count(*), 3)       as fraud_rate_pct,
    count_if(is_alert)                               as alerts
from {{ source('gold', 'transaction_features') }}
group by 1, 2
