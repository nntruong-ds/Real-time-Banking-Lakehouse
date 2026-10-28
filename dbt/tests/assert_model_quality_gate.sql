-- CỔNG CHẤT LƯỢNG: lần chạy job gold mới nhất phải đạt tối thiểu precision/recall trên TEST
-- (đặt trong dbt_project.yml -> vars). Ai sửa code làm mô hình tệ đi -> pipeline báo đỏ.
select run_time, precision, recall
from {{ source('gold', 'model_metrics') }}
where split = 'test'
  and is_chosen
  and run_time = (select max(run_time) from {{ source('gold', 'model_metrics') }})
  and (precision < {{ var('min_precision') }} or recall < {{ var('min_recall') }})
