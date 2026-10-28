-- Gian lận theo NGÀY: bao nhiêu giao dịch, bao nhiêu vụ gian lận thật, hệ thống cảnh báo
-- bao nhiêu và chặn được bao nhiêu tiền. Một dòng = một ngày x một phần dữ liệu (train/test).
select
    cast(event_time as date)                                   as event_date,
    split,
    count(*)                                                   as transactions,
    round(sum(amount), 2)                                      as amount_total,
    sum(is_fraud)                                              as frauds,
    count_if(is_alert)                                         as alerts,
    count_if(is_alert and is_fraud = 1)                        as true_alerts,
    round(sum(case when is_fraud = 1 then amount else 0 end), 2)                  as fraud_amount,
    round(sum(case when is_alert and is_fraud = 1 then amount else 0 end), 2)     as fraud_amount_caught
from {{ source('gold', 'transaction_features') }}
group by 1, 2
