{{ config(
    materialized='incremental',
    unique_key=['date', 'equipment_id'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="(date, equipment_id)"
) }}

WITH daily_status AS (
    SELECT
        toDate(timestamp) AS date,
        equipment_id,
        -- Count the number of "active" statuses per day
        countIf(status = 'active') AS active_count,
        -- Total readings per day
        count(*) AS total_count,
        countIf(status = 'active') /count(*) AS active_rate,
        -- Calculate average fuel consumption
        avg(fuel_consumption) AS avg_fuel_consumption,
        max(ingested_at) as ingested_at,
        toUnixTimestamp(now()) as version
    FROM {{ source('staging', 'equipment_sensors') }} FINAL
    GROUP BY
        date,
        equipment_id
),
-- window function to fill empty fuel consumption record
filled_status AS (
    SELECT
        date,
        equipment_id,
        active_count,
        total_count,
        active_rate,
        -- if today's data is missing, use the last known value
        COALESCE(avg_fuel_consumption, LAST_VALUE(avg_fuel_consumption) OVER (PARTITION BY equipment_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) as avg_fuel_consumption,
        ingested_at,
        version
    FROM daily_status
)
SELECT * FROM filled_status