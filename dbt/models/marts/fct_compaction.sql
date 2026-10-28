-- Hiệu quả bảo trì Iceberg cho từng bảng: lấy lần gộp file "thật" (lần có nhiều file nhất
-- TRƯỚC khi gộp), vì các lần chạy lại sau đó không còn gì để gộp.
-- Chưa từng chạy job bảo trì (chưa có bảng ops.maintenance_log) -> mart rỗng, không lỗi.
{% set log = adapter.get_relation(database='iceberg', schema='ops', identifier='maintenance_log') %}
{% if log %}
select table_name, files_before, files_after, mb_before, mb_after,
       query_sec_before, query_sec_after, run_time
from (
    select *, row_number() over (partition by table_name order by files_before desc, run_time) as rn
    from {{ log }}
) ranked
where rn = 1
{% else %}
select cast(null as varchar) as table_name, cast(null as bigint) as files_before,
       cast(null as bigint) as files_after, cast(null as double) as mb_before,
       cast(null as double) as mb_after, cast(null as double) as query_sec_before,
       cast(null as double) as query_sec_after, cast(null as timestamp(6) with time zone) as run_time
where false
{% endif %}
