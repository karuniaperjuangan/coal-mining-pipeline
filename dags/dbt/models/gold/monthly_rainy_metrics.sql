{{ config(
    materialized='incremental',
    unique_key=['month'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="(month)"
) }}
-- rainy days vs non rainy days prouction
SELECT
    toYYYYMM(d.date) AS month,
    -- production on rainy days
    avgIf(total_production_daily, precipitation_sum >= 0.1) AS rainy_days_production,
    -- production on non rainy days
    avgIf(total_production_daily, precipitation_sum < 0.1) AS non_rainy_days_production,
    max(d.ingested_at) as ingested_at,
    max(toUnixTimestamp(now())) as version
FROM {{ ref('daily_production_metrics') }} d FINAL
GROUP BY month