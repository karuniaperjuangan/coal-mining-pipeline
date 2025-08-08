SELECT * 
FROM {{ ref('daily_production_metrics') }}
WHERE total_production_daily < 0 -- failing if there is any negative total_production_daily