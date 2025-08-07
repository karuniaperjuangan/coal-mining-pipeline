{{ config(
    materialized='table',
    unique_key=['date', 'mine_id', 'shift'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="('date', 'mine_id')"
) }}

SELECT
    date,
    mine_id,
    -- replace negative tons_extracted with 0
    SUM(CASE WHEN tons_extracted < 0 THEN 0 ELSE tons_extracted END) AS total_tons_extracted,
    tons_extracted < 0 AS is_anomaly,
    SUM(quality_grade * tons_extracted) / SUM(CASE WHEN tons_extracted < 0 THEN 0 ELSE tons_extracted END) AS avg_quality_grade,
    MAX(ingested_at) as ingested_at,
    toUnixTimestamp(now()) as version
FROM {{ source('staging', 'production_logs') }}
LEFT JOIN {{ source('staging', 'mines') }}
GROUP BY date, mine_id