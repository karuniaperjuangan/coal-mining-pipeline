SELECT * 
FROM {{ ref('daily_production_metrics') }}
WHERE equipment_utilization < 0 OR equipment_utilization >1-- failing if there is equipment utilization range