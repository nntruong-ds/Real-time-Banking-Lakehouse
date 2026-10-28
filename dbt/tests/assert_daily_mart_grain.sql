-- Mỗi (ngày, train/test) chỉ được có MỘT dòng trong fct_daily_fraud.
select event_date, split, count(*) as so_dong
from {{ ref('fct_daily_fraud') }}
group by 1, 2
having count(*) > 1
