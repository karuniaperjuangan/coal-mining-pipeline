{{ config(
    materialized='table',
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="(date)"
) }}
WITH daily_production AS (
    SELECT
    date,
    sum(total_tons_extracted) AS total_tons_extract,
    max(has_anomaly) AS has_anomaly,
    sum(avg_quality_grade * greatest(0,total_tons_extracted)) / sum(greatest(0,total_tons_extracted)) AS avg_quality_grade,
    max(ingested_at) as ingested_at,
    max(toUnixTimestamp(now())) as version
FROM {{ ref('int_daily_mine_production') }} FINAL
GROUP BY date
),
daily_equipment AS (
    SELECT
        date,
        avg(active_rate) AS active_rate,
        -- if today's data is missing, use the last known value
        avg(avg_fuel_consumption) as avg_fuel_consumption,
        max(ingested_at),
         max(version) as version
    FROM {{ ref('int_daily_equipment') }} FINAL
    GROUP BY date
)
SELECT
    w.date AS date,
    w.temperature_2m_mean AS temperature_2m_mean,
    w.precipitation_sum AS precipitation_sum,
    e.active_rate AS equipment_utilization,
    e.avg_fuel_consumption AS equipment_avg_fuel_consumption,
    m.total_tons_extract AS total_production_daily,
    m.avg_quality_grade AS average_quality_grade,
    w.ingested_at as ingested_at,
    toUnixTimestamp(now()) as version    
FROM {{ ref('int_daily_weather') }} w FINAL
LEFT JOIN daily_production m USING (date)
LEFT JOIN daily_equipment e USING (date)