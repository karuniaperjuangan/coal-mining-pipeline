{{ config(
    materialized='table',
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="(date)"
) }}

SELECT
    toDate(time) AS date,
    avg(temperature_2m_mean) AS temperature_2m_mean,
    sum(precipitation_sum) AS precipitation_sum,
    max(ingested_at) as ingested_at,
    max(toUnixTimestamp(now())) as version    
FROM {{ source('staging', 'weather') }}
GROUP BY toDate(time)