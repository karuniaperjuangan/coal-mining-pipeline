SELECT
    *   
FROM  {{ source('staging', 'weather') }} FINAL