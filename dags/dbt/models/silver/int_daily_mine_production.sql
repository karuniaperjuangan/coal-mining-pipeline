{{ config(
    materialized='incremental',
    unique_key=['date', 'mine_id'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="(date, mine_id)"
) }}

SELECT
    date,
    mine_id,
    -- fix anomaly: replace negative tons_extracted with 0
    sum(greatest(0,tons_extracted)) AS total_tons_extracted,
    sum(tons_extracted < 0) >0 AS has_anomaly,
    sum(quality_grade * greatest(0,tons_extracted)) / sum(greatest(0,tons_extracted)) AS avg_quality_grade,
    max(ingested_at) as ingested_at,
    max(toUnixTimestamp(now())) as version
FROM {{ ref('production_logs') }}
LEFT JOIN {{ ref('mines') }} using (mine_id)
GROUP BY date, mine_id