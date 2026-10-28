{{ config(materialized='ephemeral') }}
-- Độ trễ của TỪNG giao dịch đi qua luồng streaming:
--   từ lúc producer gửi vào Kafka (kafka_timestamp, ghi ở Bronze)
--   tới lúc giao dịch đã nằm trong Silver (silver_time = lúc mẻ streaming ghi xong).
-- Bronze có thể có bản trùng (producer cố ý gửi trùng) -> lấy lần gửi ĐẦU TIÊN.
with sent as (
    select transaction_id, min(kafka_timestamp) as kafka_ts
    from {{ source('bronze', 'transactions_raw') }}
    where ingest_source = 'stream'
    group by transaction_id
)
select
    s.transaction_id,
    sent.kafka_ts,
    s.silver_time,
    to_unixtime(s.silver_time) - to_unixtime(sent.kafka_ts) as latency_sec
from {{ source('silver', 'transactions') }} s
join sent on s.transaction_id = sent.transaction_id
where s.ingest_source = 'stream'
