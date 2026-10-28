-- ĐỐI SOÁT Silver -> Gold: cùng số giao dịch, cùng tổng tiền (lệch tối đa 1 xu).
-- Thất bại khi Silver có dữ liệu mới mà job gold chưa chạy lại.
with s as (
    select count(*) as n, round(sum(amount), 2) as amount from {{ source('silver', 'transactions') }}
),
g as (
    select count(*) as n, round(sum(amount), 2) as amount
    from {{ source('gold', 'transaction_features') }}
)
select s.n as silver_rows, g.n as gold_rows, s.amount as silver_amount, g.amount as gold_amount
from s cross join g
where s.n <> g.n or abs(s.amount - g.amount) > 0.01
