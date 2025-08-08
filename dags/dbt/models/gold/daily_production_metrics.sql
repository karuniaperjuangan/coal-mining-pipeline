{{ config(
    materialized='incremental',
    unique_key=['date'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="(date)"
) }}
SELECT
    m.date AS date,
    avg(w.temperature_2m_mean) AS temperature_2m_mean,
    sum(w.precipitation_sum) AS precipitation_sum,
    avg(e.active_rate) AS equipment_utilization,
    avg(e.avg_fuel_consumption) AS equipment_avg_fuel_consumption,
    sum(m.total_tons_extracted) AS total_production_daily,
    avg(m.avg_quality_grade) AS average_quality_grade,
    max(greatest(w.ingested_at,m.ingested_at,e.ingested_at)) as ingested_at,
    max(toUnixTimestamp(now())) as version    
FROM {{ ref('int_daily_mine_production') }} m
LEFT JOIN {{ ref('int_daily_weather') }} w USING (date)
LEFT JOIN {{ ref('int_daily_equipment') }} e USING (date)
GROUP BY w.date