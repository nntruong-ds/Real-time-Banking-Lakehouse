-- kpi_summary phải có ĐÚNG một dòng (dashboard đọc các ô số từ dòng này).
select count(*) as so_dong
from {{ ref('kpi_summary') }}
having count(*) <> 1
