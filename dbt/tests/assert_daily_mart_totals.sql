-- Mart theo ngày cộng lại phải bằng đúng bảng Gold: không mất dòng khi gom nhóm.
with m as (
    select sum(transactions) as n, sum(true_alerts) as tp from {{ ref('fct_daily_fraud') }}
),
g as (
    select count(*) as n, count_if(is_alert and is_fraud = 1) as tp
    from {{ source('gold', 'transaction_features') }}
)
select m.n as mart_transactions, g.n as gold_transactions,
       m.tp as mart_true_alerts, g.tp as gold_true_alerts
from m cross join g
where m.n <> g.n or m.tp <> g.tp
