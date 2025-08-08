SELECT
    *   
FROM  {{ source('staging', 'equipment_sensors') }} FINAL