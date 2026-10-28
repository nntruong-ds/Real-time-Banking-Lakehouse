-- Hàng đợi cảnh báo cho người duyệt: 500 giao dịch điểm cao nhất trên dữ liệu TEST,
-- lý do nối thành một chuỗi để dashboard hiển thị gọn.
select
    transaction_id,
    event_time,
    card_last4,
    merchant,
    category,
    amount,
    round(score, 2)                     as score,
    array_join(reasons, ', ')           as reasons_text,
    is_fraud                            as is_true_fraud
from {{ source('gold', 'fraud_alerts') }}
where split = 'test'
order by score desc
limit 500
