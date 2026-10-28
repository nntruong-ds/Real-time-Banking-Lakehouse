-- Bảng cảnh báo phải đúng bằng tập giao dịch có is_alert = true trong bảng đặc trưng:
-- không thiếu cảnh báo nào, không có cảnh báo "ma".
with f as (
    select transaction_id from {{ source('gold', 'transaction_features') }} where is_alert
),
a as (
    select transaction_id from {{ source('gold', 'fraud_alerts') }}
)
select coalesce(f.transaction_id, a.transaction_id) as transaction_id,
       case when a.transaction_id is null then 'thieu trong fraud_alerts'
            else 'thua trong fraud_alerts' end as van_de
from f full outer join a on f.transaction_id = a.transaction_id
where f.transaction_id is null or a.transaction_id is null
