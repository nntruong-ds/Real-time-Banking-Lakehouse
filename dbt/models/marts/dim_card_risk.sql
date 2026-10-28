-- Hồ sơ rủi ro theo THẺ (card_token, đã che số thẻ): đội chống gian lận dùng để biết thẻ nào
-- đã bị lộ (có gian lận thật), thẻ nào cần theo dõi (từng bị cảnh báo).
select
    card_token,
    max(card_last4)                                  as card_last4,
    count(*)                                         as transactions,
    round(sum(amount), 2)                            as amount_total,
    round(avg(amount), 2)                            as amount_avg,
    sum(is_fraud)                                    as frauds,
    count_if(is_alert)                               as alerts,
    round(max(score), 3)                             as max_score,
    min(event_time)                                  as first_seen,
    max(event_time)                                  as last_seen,
    case
        when sum(is_fraud) > 0   then 'compromised'
        when count_if(is_alert) > 0 then 'watch'
        else 'normal'
    end                                              as risk_status
from {{ source('gold', 'transaction_features') }}
group by card_token
