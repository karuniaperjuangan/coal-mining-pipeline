SELECT
    *   
FROM  {{ source('staging', 'mines') }} FINAL