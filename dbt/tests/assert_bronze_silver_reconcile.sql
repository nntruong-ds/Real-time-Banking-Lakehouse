-- ĐỐI SOÁT Bronze -> Silver. Test trả về dòng nào = THẤT BẠI.
-- Mỗi giao dịch hợp lệ ở Bronze (đếm theo transaction_id, vì Bronze cố ý giữ cả bản trùng)
-- phải có đúng một dòng ở Silver: không mất giao dịch, không thừa giao dịch.
-- Chạy khi luồng streaming đã đuổi kịp (Silver có thể chậm hơn Bronze vài chục giây).
with bronze as (
    select count(distinct transaction_id) as n from {{ source('bronze', 'transactions_raw') }}
),
silver as (
    select count(*) as n from {{ source('silver', 'transactions') }}
)
select bronze.n as bronze_distinct_ids, silver.n as silver_rows, bronze.n - silver.n as lech
from bronze cross join silver
where bronze.n <> silver.n
