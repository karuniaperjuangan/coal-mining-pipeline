{{ config(
    materialized='table',
    unique_key=['date', 'equipment_id'],
    engine="ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', version)",
    order_by="('date')"
) }}

SELECT
    *
FROM {{ source('staging', 'weather') }}