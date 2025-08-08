SELECT
    *   
FROM  {{ source('staging', 'production_logs') }} FINAL