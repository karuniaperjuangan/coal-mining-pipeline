{{ config(
    materialized='table',
    unique_key=['date', 'equipment_id'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="('date', 'equipment_id')"
) }}

WITH daily_status AS (
    SELECT
        toDate(timestamp) AS date,
        equipment_id,
        -- Count the number of "active" statuses per day
        COUNTIF(status = 'active') AS active_count,
        -- Total readings per day
        COUNT(*) AS total_count,
        -- Calculate average fuel consumption
        AVG(fuel_consumption) AS avg_fuel_consumption,
        MAX(ingested_at) as ingested_at,
        toUnixTimestamp(now()) as version
    FROM {{ source('staging', 'equipment_sensors') }}
    GROUP BY
        date,
        equipment_id
),
-- Use a window function to fill missing data with the previous day's average
filled_status AS (
    SELECT
        date,
        equipment_id,
        active_count,
        total_count,
        avg_fuel_consumption,
        -- If today's data is missing, use the last known value
        COALESCE(avg_fuel_consumption, LAST_VALUE(avg_fuel_consumption) OVER (PARTITION BY equipment_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) as filled_avg_fuel_consumption,
        ingested_at,
        version
    FROM daily_status
)
SELECT * FROM filled_status