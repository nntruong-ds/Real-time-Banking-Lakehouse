-- MỘT dòng chứa mọi con số chính cho dashboard (và cho CV):
-- kết quả mô hình trên TEST, tiền gian lận chặn được, độ trễ streaming p50/p95.
with model as (
    select run_time, precision, recall, f1, roc_auc, alerts
    from {{ source('gold', 'model_metrics') }}
    where split = 'test'
      and is_chosen
      and run_time = (select max(run_time) from {{ source('gold', 'model_metrics') }})
),
money as (
    select sum(frauds)               as frauds,
           sum(true_alerts)          as frauds_caught,
           sum(fraud_amount)         as fraud_amount,
           sum(fraud_amount_caught)  as fraud_amount_caught
    from {{ ref('fct_daily_fraud') }}
    where split = 'test'
),
latency as (
    select round(approx_percentile(latency_sec, 0.5), 1)  as latency_p50_sec,
           round(approx_percentile(latency_sec, 0.95), 1) as latency_p95_sec
    from {{ ref('int_stream_latency') }}
)
select
    model.run_time                                                        as model_run_time,
    round(model.precision, 3)                                             as precision,
    round(model.recall, 3)                                                as recall,
    round(model.f1, 3)                                                    as f1,
    round(model.roc_auc, 3)                                               as roc_auc,
    model.alerts,
    money.frauds,
    money.frauds_caught,
    round(money.fraud_amount, 0)                                          as fraud_amount,
    round(money.fraud_amount_caught, 0)                                   as fraud_amount_caught,
    round(100.0 * money.fraud_amount_caught / nullif(money.fraud_amount, 0), 1)
                                                                          as pct_fraud_amount_caught,
    latency.latency_p50_sec,
    latency.latency_p95_sec
from model
cross join money
cross join latency
